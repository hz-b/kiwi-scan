# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
import os
import queue
import random
import string
import threading
import time

# import pdb
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

from kiwi_scan.actuator.factory import create_actuator
from kiwi_scan.actuator.single import AbstractActuator, PvEvent
from kiwi_scan.data.loader import DataLoader, resolve_data_dir
from kiwi_scan.data.manifestwriter import ManifestWriter
from kiwi_scan.datamodels import (
    ActuatorConfig,
    ScanConfig,
    ScanDimension,
    SubscriptionConfig,
)
from kiwi_scan.epics_wrapper import EpicsPV, ensure_ca_context
from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.monitor.factory import create_monitor
from kiwi_scan.plugin.registry import create_plugin
from kiwi_scan.scan.scan_abs import ScanABC

from .column_provider import DataColumnProvider
from .metadata_monitor import MetadataCAMonitor
from .subscription_manager import SubscriptionManager
from .sync_controller import SyncController
from .trigger_manager import TriggerManager

logger = logging.getLogger(__name__)

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
        ensure_ca_context()
        super().__init__(config, data_dir)
        logger.debug("Init BaseScan")
        self.busyflag = False
        self.cfg = config
        # Validate 
        self.cfg.validate()
        self.scan_type = self.__class__.__name__
        # Perform config cleanup
        self._validate_and_filter_actuators()

        self.plugins = [create_plugin(plugin_config, self) for plugin_config in self.cfg.plugin_configs]
        logger.debug("Plugin Configs: %s", self.cfg.plugin_configs)

        # Perform config cleanup
        self._validate_and_filter_actuators()
        # normalize optionals
        self.scan_dimensions = config.scan_dimensions or []
        self.parallel_scans  = config.parallel_scans  or []
        self.nested_scans    = config.nested_scans    or []
        self.trigger_manager = TriggerManager.from_config(self.cfg.triggers)
        # Prepare I/O
        self.data_dir = os.path.abspath(resolve_data_dir(data_dir, config.data_dir))
        logger.info("Data directory: %s", self.data_dir)

        self._data_writer_lock = threading.RLock()
        self._requested_output_file = config.output_file
        self._output_timestamp = datetime.now().astimezone().strftime("%Y%m%d%H%M%S")
        self._data_writing_enabled = bool(
            getattr(config, "data_writing_enabled", True)
        )
        self._data_header_written = False

        # copy runtime flags
        self.include_timestamps = config.include_timestamps
        self.sample_rate_hz = 1.0
        self.sampletime = 1.0
        self._apply_sample_rate(getattr(config, "sample_rate_hz", None))

        self.debug = config.debug
        # if self.debug:
        #    pdb.set_trace()
        self.output_file: Optional[str] = None
        if self._data_writing_enabled:
            self._ensure_output_file_exists()
        # setup
        logger.debug("_connect_detectors")
        self._connect_detectors()
        logger.debug("_connect_actuators")
        self._connect_actuators()
        self.actuators = getattr(self, "actuators", {})
        logger.debug("init subscription manager")
        self.subscription_manager = SubscriptionManager(
            getattr(self.cfg, "subscriptions", None) or [],
            actuator_configs=getattr(self.cfg, "actuators", {}) or {},
            actuators=self.actuators,
        )
        self.sync_controller = SyncController(
            getattr(self.cfg, "subscriptions", None) or []
        )
        logger.debug("_validate_config")
        
        self._validate_config()
        if config.stop_pv:
            self.stop_pv = EpicsPV(config.stop_pv)
            self.prefix = config.stop_pv.split(':')[0]
        else:
            self.stop_pv = None
        # Build a time-stamped sibling file next to main scan file
        base_name, ext = os.path.splitext(self.cfg.metadata_file or "scan_metadata.txt")
        # Reuse the same timestamp as the main file when writing is enabled.
        self._metadata_out = os.path.join(self.data_dir, f"{base_name}-{self._output_timestamp}{ext}")
        # Create the monitor (but don't start yet)
        self._meta_mon = MetadataCAMonitor(
            pvs=list(self.cfg.metadata_pvs or []),
            constants=dict(self.cfg.metadata_constants or {}),
            outfile=self._metadata_out,
            queue_maxsize=20000, 
        )
        self._meta_mon_started = False
        self._position: Any = None
        self._last_point: Dict[str, Any] = {}
        self._current_row_cache: Dict[str, Any] = {}
        self._data_column_providers: List[DataColumnProvider] = []
        self._daq_is_on = False   # safe to take data for stats
        self.integration_time = config.integration_time
        self._perf_enabled: bool = bool(
            getattr(self.cfg, "debug", False)
            or getattr(self.cfg, "performance_report", False)
        )
        self._manifest_mode = getattr(self.cfg, "manifest_mode", "full")
        if self._perf_enabled:
            logger.info("Performance reporting enabled")
        else:
            logger.debug("Performance report disabled")
        self._perf: Dict[str, List[float]] = defaultdict(list)
        
        # --- event-driven wakeup state (used in _on_heartbeat_event()) with condition  ---
        self._tick_cond = threading.Condition()
        self._tick_seq = 0  # increments on each heartbeat event
        # --- event driven stop event semaphore, can be checked non blocking with is_set()
        self._stop_requested = threading.Event()
        # optional: last-seen events for debugging
        self._last_heartbeat: Optional[PvEvent] = None
        self._last_sync: Optional[PvEvent] = None
        self._last_status: Optional[PvEvent] = None

        # Creating trigger worker thread to avoid caput from callback context.
        self._trigger_q = queue.SimpleQueue()
        self._trigger_worker_stop = threading.Event()
        self._trigger_worker = threading.Thread(
            target=self._trigger_worker_loop,
            daemon=True,
        )
        self._trigger_worker.start()
        # Creating plugin worker thread to avoid caput from callback context.
        self._plugin_q = queue.SimpleQueue()
        self._plugin_worker_stop = threading.Event()
        self._plugin_worker = threading.Thread(
            target=self._plugin_worker_loop,
            daemon=True,
        )
        self._plugin_worker.start()
        
        # TODO: cleanup, use  _start_subscriptions ouside
        if getattr(self, "ROLE_CALLBACKS", None):
            logger.debug("Detected legacy ROLE_CALLBACKS on %s; auto-starting subscriptions for compatibility", type(self).__name__)
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
                logger.debug("[PERF] idx=%d %-20s %.6f s", idx, name, dt)
            else:
                logger.debug("[PERF] %-20s %.6f s", name, dt)

    def _perf_report(self) -> None:
        """Print a compact summary once at the end of a scan."""
        if not self._perf_enabled:
            return

        metadata_queue_drops = self.get_metadata_queue_drop_count()

        def p95(values: List[float]) -> float:
            if not values:
                return 0.0
            vs = sorted(values)
            k = int(0.95 * (len(vs) - 1))
            return vs[k]

        print("========== PERFORMANCE SUMMARY ==========")
        for name, values in sorted(self._perf.items()):
            n = len(values)
            if n == 0:
                continue
            total = sum(values)
            mean = total / n
            mx = max(values)

            print(
                f"[PERF] {name:<20} "
                f"n={n} total={total:.3f}s mean={mean:.6f}s "
                f"p95={p95(values):.6f}s max={mx:.6f}s"
            )
            logger.info("[PERF] %-20s n=%d total=%.3fs mean=%.6fs p95=%.6fs max=%.6fs", name, n, total, mean, p95(values), mx)
        print(f"[PERF] {'metadata_queue_drops':<20} count={metadata_queue_drops}")
        logger.info("[PERF] %-20s count=%d", "metadata_queue_drops", metadata_queue_drops)
        print("==========================================")

    # -------------------- subscription/callback integration --------------------

    def register_subscription_role(
        self,
        role: str,
        handler: Callable[[PvEvent, SubscriptionConfig], None],
    ) -> None:
        self.subscription_manager.register_role(role, handler)

    def _start_subscriptions(self) -> None:
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

    def _wait_for_sync(self, stop_event=None) -> bool:
        ok = self.sync_controller.wait(stop_event=stop_event)
        if not ok:
            logger.debug("SyncController wait aborted (required=%s)", list(self.sync_controller.required_names))
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
            logger.warning(f"Removing unused actuators not referenced in scan_dimensions: {unused_actuators}")
            for name in unused_actuators:
                del self.cfg.actuators[name]

        # Ensure every dimension references a valid actuator
        for dim in self.cfg.scan_dimensions:
            if dim.actuator not in self.cfg.actuators:
                raise ValueError(f"ScanDimension refers to unknown actuator: '{dim.actuator}'")

    def _connect_detectors(self):
        logger.debug(f"Detector PVs: {self.cfg.detector_pvs}")
        logger.debug("Init Detectors")
        logger.debug(f"Monitor: {self.cfg.detector_pvs_monitor}")

        self.detector_pvs = []
        for i, pvname in enumerate(self.cfg.detector_pvs):
            logger.debug("Creating detector PV %d/%d: %s", i+1, len(self.cfg.detector_pvs), pvname)
            pv = EpicsPV(
                pvname,
                timeout=1.0,
                connection_timeout=1.0,
                queueing_delay=0.0,
                auto_monitor=True,
            )
            logger.debug("Created detector PV: %s", pvname)
            self.detector_pvs.append(pv)
            self.detector_pvs_monitor = self.cfg.detector_pvs_monitor;

    def _connect_actuators(self):
        logger.debug("Init Actuators")
        # 1. Check for at least one actuator
        if not getattr(self.cfg, "actuators", None):
            logger.info("No actuators have been configured!")
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

            logger.info(f"Creating actuator '{name}' → PV='{cfg.pv}', RB_PV='{cfg.rb_pv}'")
            # instantiate the Actuator
            act = create_actuator(cfg)
            logger.debug("Actuator created")
            actuators[name] = act

        # assign the full dict back onto self
        self.actuators = actuators
        logger.info(f"Number of actuators: {len(self.actuators)}")

    def _validate_config(self):
        if not (self.scan_dimensions or self.parallel_scans or self.nested_scans):
            raise ValueError("No scan dimensions provided in ScanConfig.")

    def _apply_sample_rate(self, sample_rate_hz: Optional[float]) -> None:
        """Store the current scan sample rate and derived sample period."""
        if sample_rate_hz is None:
            sample_rate_hz = 1.0

        rate = float(sample_rate_hz)
        if rate <= 0.0:
            logger.error(f"sample_rate_hz must be positive, got {rate}")
            rate = rate * -1.0

        self.sample_rate_hz = rate
        self.sampletime = 1.0 / rate

    def set_samplerate( self, dim: Optional[ScanDimension] = None, sample_rate_hz: Optional[float] = None):
        """ Set the scan sample rate.  """

        rate_hz = sample_rate_hz
        if rate_hz is None:
            rate_hz = getattr(self.cfg, "sample_rate_hz", 1.0)
        self._apply_sample_rate(rate_hz)
        self.sync_controller.set_timer_period(self.sampletime)
    
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("delay = %.6f, start time = %s, scheduled time = %s", delay,
                datetime.fromtimestamp(start_time, tz=timezone.utc).strftime( "%H:%M:%S.%f"),
                datetime.fromtimestamp( scheduled_sample_time, tz=timezone.utc,).strftime("%H:%M:%S.%f"),
            )

    def get_data_writing_enabled(self) -> bool:
        with self._data_writer_lock:
            return self._data_writing_enabled

    def set_data_writing_enabled(self, enabled: bool) -> None:
        """
        Enable/disable main scan file writing and metadata sidecar logging.

        Disabling is immediate:
          - no further scan rows are appended
          - an active metadata sidecar monitor is stopped

        Re-enabling during a running scan is also supported:
          - the main output file is created lazily on the next written point
          - metadata monitoring is restarted if the scan is busy
        """
        enabled = bool(enabled)
        with self._data_writer_lock:
            if self._data_writing_enabled == enabled:
                return
            self._data_writing_enabled = enabled

        if enabled:
            logger.info("Data writing enabled")
            if self.busy:
                self._start_metadata_monitor()
        else:
            logger.info("Data writing disabled")
            self._stop_metadata_monitor()

    def _ensure_output_file_exists(self) -> Optional[str]:
        """Create the timestamped output file lazily when writing is enabled."""
        with self._data_writer_lock:
            if not self._data_writing_enabled:
                return None
            if self.output_file is None:
                self.output_file = self.generate_and_create_file(
                    self._requested_output_file
                )
            return self.output_file

    def generate_and_create_file(self, base_filename):
        """
        Generates a new filename by appending the current date and time to the base filename.
        Parameters:
            base_filename (str): The original filename (e.g., 'monotest.txt').
        Returns:
            str: The new filename with the timestamp appended (e.g., 'monotest-202411061655.txt').
        """
        while True:
            timestamp = self._output_timestamp
            name, ext = os.path.splitext(base_filename)
            new_filename = os.path.join(self.data_dir, f"{name}-{timestamp}{ext}")
            if not os.path.exists(new_filename):
                with open(new_filename, 'w'):
                    pass  # Create an empty file
                return new_filename
            # If the file exists, add a random 6-character suffix and retry
            random_suffix = "".join(random.choices(string.ascii_letters + string.digits, k=6))  # nosec B311
            new_filename = os.path.join(self.data_dir, f"{name}-{timestamp}_{random_suffix}{ext}")
            
            if not os.path.exists(new_filename):
                with open(new_filename, 'w'):
                    pass  # Create an empty file
                return new_filename

    def get_output_file(self):
        return self.output_file

    def _start_metadata_monitor(self) -> None:
        if not self.get_data_writing_enabled():
            logger.info("Metadata monitor not started: data writing is disabled")
            return
        if self._meta_mon_started:
            return
        try:
            self._meta_mon.start()
            self._meta_mon_started = True
            logger.info("Started metadata task")
        except Exception:
            logger.exception("Failed to start metadata monitor")

    def _stop_metadata_monitor(self) -> None:
        if not self._meta_mon_started:
            return
        try:
            self._meta_mon.stop()
        except Exception:
            logger.exception("Error stopping metadata monitor")
        finally:
            self._meta_mon_started = False

    def get_metadata_queue_drop_count(self) -> int:
        """Return metadata monitor queue drops for diagnostics/performance reports."""
        monitor = getattr(self, "_meta_mon", None)
        if monitor is None:
            return 0

        getter = getattr(monitor, "get_drop_count", None)
        if callable(getter):
            try:
                return int(getter())
            except Exception:
                logger.debug("Failed to read metadata monitor drop count", exc_info=True)
                return 0

        try:
            return int(getattr(monitor, "dropped_events", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def read_detectors(self) -> List[Any]:
        """
        Read values (with metadata) from all detector PVs.
        Settings from top level yaml config:
            'set detector_pvs_monitor: True':  Read from cache for DAQ! 
            'set detector_pvs_monitor: False':  Network performance tests.
        Warning: PV returns `None`
        Error:  Any exception during the get is logged including traceback
        Returns:
            List[Any]:
                A list of “readings” where each element is `pv.get_with_metadata()`.   
        """
        readings: List[Any] = []
        for pv in self.detector_pvs:
            try:
                # >>>> Read from cache for DAQ! 'set detector_pvs_monitor: False' only for network performance tests <<<< !
                reading = pv.get_with_metadata(use_monitor=self.detector_pvs_monitor) 
                if reading is None:
                    logger.warning("Received None for PV %s", pv.pvname)
                    readings.append(None)
                else:
                    readings.append(reading)
            except Exception as e:  # noqa BLE001 
                logger.error("Failed to read metadata for PV %s, %s", pv.pvname, e)
                readings.append(None)
            # logger.debug("PV %s → %r", pv.pvname, reading)
        return readings

    # -------------------- data column provider integration --------------------

    def add_column_provider(self, provider: DataColumnProvider) -> None:
        """Register an object that contributes dynamic scan-file columns."""
        if provider is None:
            return
        if not hasattr(self, "_data_column_providers"):
            self._data_column_providers = []
        self._data_column_providers.append(provider)

    def _get_data_column_providers(self) -> List[DataColumnProvider]:
        return list(getattr(self, "_data_column_providers", []) or [])

    def _get_data_column_headers(self, include_timestamps: bool) -> List[str]:
        headers: List[str] = []
        for provider in self._get_data_column_providers():
            try:
                headers += list(provider.get_headers(include_timestamps))
            except Exception:
                logger.exception("Failed to read data column provider headers from %s", provider)
        return headers

    def _get_data_column_values(self) -> List[Any]:
        values: List[Any] = []
        for provider in self._get_data_column_providers():
            try:
                values += list(provider.get_values())
            except Exception:
                logger.exception("Failed to read data column provider values from %s", provider)
        return values

    def _build_detector_headers(self, include_timestamps: bool) -> List[str]:
        """Return detector column headers in file order."""
        if include_timestamps:
            return [
                item
                for pv in self.detector_pvs
                for item in (pv.pvname, f"TS-ISO8601-{pv.pvname}")
            ]
        return [pv.pvname for pv in self.detector_pvs]

    def _build_plugin_headers(self, include_timestamps: bool) -> List[str]:
        """Return plugin column headers in file order."""
        plugin_headers: List[str] = []
        for plugin in self.plugins:
            plugin_headers += plugin.get_headers(include_timestamps)
        return plugin_headers

    def build_monitor_signal_names(self) -> List[str]:
        """Return the logical value columns consumed by monitor.update()."""
        signal_names: List[str] = [pv.pvname for pv in self.detector_pvs]
        for plugin in self.plugins:
            signal_names += plugin.get_headers(False)
        logger.debug("Built monitor signal names: %s", signal_names)
        return signal_names

    def build_output_headers(self, include_timestamps: Optional[bool] = None) -> List[str]:
        """
        Build the full scan-file header list in one place.
        The returned list matches the order written by `write_header_to_output_file`.
        """
        if include_timestamps is None:
            include_timestamps = self.include_timestamps

        headers: List[str] = ["Position"]

        provider_headers = self._get_data_column_headers(include_timestamps)
        if provider_headers:
            headers += provider_headers

        headers += ["TS-ISO8601"]
        headers += self._build_detector_headers(include_timestamps)
        headers += self._build_plugin_headers(include_timestamps)

        logger.debug("Built output headers: %s", headers)
        return headers

    def _timestamp_to_iso(self, timestamp: Any) -> str:
        """Return a scan-output ISO timestamp string for a POSIX timestamp."""
        if timestamp is None:
            return ""
        try:
            return datetime.fromtimestamp(float(timestamp)).astimezone().isoformat()
        except (TypeError, ValueError, OSError, OverflowError):
            return str(timestamp)

    def build_output_row_values(
        self,
        position: Any,
        detector_values: List[Any],
        include_timestamps: Optional[bool] = None,
        *,
        line_ts_iso: Optional[str] = None,
        provider_values: Optional[List[Any]] = None,
    ) -> List[Any]:
        """Build one concrete scan row in the same order as build_output_headers().

        The returned row is intentionally unformatted. The scan-file writer
        applies its normal text formatting, while monitors can use the same
        values with their own formatter.
        """
        if include_timestamps is None:
            include_timestamps = self.include_timestamps
        if line_ts_iso is None:
            line_ts_iso = datetime.now().astimezone().isoformat()
        if provider_values is None:
            provider_values = self._get_data_column_values()

        row: List[Any] = [position]
        row += list(provider_values or [])
        row.append(line_ts_iso)

        for item in detector_values or []:
            if isinstance(item, dict):
                value = item.get("value")
                timestamp = item.get("timestamp")
            else:
                value = item
                timestamp = None

            row.append(value)
            if include_timestamps:
                row.append(self._timestamp_to_iso(timestamp))

        return row

    @staticmethod
    def _plain_scan_value(item: Any) -> Any:
        """Return the scalar value stored in one scan value object.

        Detector and plugin values are normally mappings with a ``value`` key,
        while tests and simple integrations may pass raw scalars.  The current
        row cache stores scalar values so expression plugins can use them
        directly.
        """
        if isinstance(item, dict):
            return item.get("value")
        return item

    def update_current_row_cache(
        self,
        *,
        idx: int,
        pos: Any,
        values: List[Any],
        headers: Optional[List[str]] = None,
        provider_values: Optional[List[Any]] = None,
        line_ts_iso: Optional[str] = None,
        clear: bool = True,
    ) -> Dict[str, Any]:
        """Update the current scan-line cache from detector values.

        This method is intended to be the one-liner used directly after
        ``read_detectors()`` and before plugin execution::

            vals = self.read_detectors()
            self.update_current_row_cache(idx=idx, pos=pos, values=vals)

        The cache is deliberately scalar-oriented: detector dictionaries and
        plugin dictionaries are reduced to their ``value`` field.  The full
        metadata-bearing values are still kept by the normal writer path.
        """
        row: Dict[str, Any] = {} if clear else dict(getattr(self, "_current_row_cache", {}) or {})

        row["idx"] = idx
        row["pos"] = pos
        row["Position"] = float(pos) if pos is not None else pos

        if line_ts_iso is not None:
            row["TS-ISO8601"] = line_ts_iso

        if provider_values is None:
            provider_values = self._get_data_column_values()
        provider_headers = self._get_data_column_headers(False)
        for name, item in zip(provider_headers, provider_values or []):
            row[str(name)] = self._plain_scan_value(item)

        if headers is None:
            headers = [pv.pvname for pv in getattr(self, "detector_pvs", [])]
        for name, item in zip(headers, values or []):
            row[str(name)] = self._plain_scan_value(item)
            if isinstance(item, dict):
                ts = item.get("timestamp")
                if ts is not None:
                    row["TS-ISO8601-" + str(name)] = self._timestamp_to_iso(ts)

        self._current_row_cache = row
        return dict(row)

    def extend_current_row_cache(
        self,
        headers: List[str],
        values: List[Any],
    ) -> Dict[str, Any]:
        """Add additional scalar columns to the current scan-line cache.

        This is useful after each plugin has produced values, so later plugins
        can use columns produced by earlier plugins in the same scan line.
        """
        row: Dict[str, Any] = dict(getattr(self, "_current_row_cache", {}) or {})
        for name, item in zip(headers or [], values or []):
            row[str(name)] = self._plain_scan_value(item)
            if isinstance(item, dict):
                ts = item.get("timestamp")
                if ts is not None:
                    row["TS-" + str(name)] = self._timestamp_to_iso(ts)
        self._current_row_cache = row
        return dict(row)

    def get_current_row_cache(self) -> Dict[str, Any]:
        """Return a copy of the current scan-line cache."""
        return dict(getattr(self, "_current_row_cache", {}) or {})

    def get_current_row_value(self, key: str, default: Any = None) -> Any:
        """Return one scalar value from the current scan-line cache."""
        return (getattr(self, "_current_row_cache", {}) or {}).get(key, default)

    def _update_data_column_provider_cache(
        self,
        last: Dict[str, Any],
        include_timestamps: bool,
    ) -> None:
        for provider in self._get_data_column_providers():
            try:
                provider.update_last_point(last, include_timestamps)
            except Exception:
                logger.exception("Failed to update last-point cache from %s", provider)

    def _reset_data_column_provider_windows(self) -> None:
        """Start a new provider data window for the next scan point."""
        for provider in self._get_data_column_providers():
            reset = getattr(provider, "reset_window", None)
            if not callable(reset):
                continue
            try:
                reset()
            except Exception:
                logger.exception("Failed to reset data column provider %s", provider)

    @staticmethod
    def _format_scan_value(value: Any) -> str:
        if value is None:
            return ""
        try:
            return f"{float(value):.12e}"
        except (ValueError, TypeError):
            return str(value)

    def save_to_file(self, position, detector_values, include_timestamps=True):
        """
        Write one scan row and return the same concrete row values.
        """
        logger.debug("Detector values to be written: %s", detector_values)

        # Independent per-line timestamp time zone aware (ISO 8601)
        line_ts_iso = datetime.now().astimezone().isoformat()
        provider_values = self._get_data_column_values()
        row_values = self.build_output_row_values(
            position,
            detector_values,
            include_timestamps,
            line_ts_iso=line_ts_iso,
            provider_values=provider_values,
        )

        # Update in-memory last-point cache (used by get_value)
        try:
            self._update_last_point_cache(
                position=position,
                line_ts_iso=line_ts_iso,
                values=detector_values,
                include_timestamps=include_timestamps,
            )
        except Exception:
            logger.debug("Failed to update last-point cache", exc_info=True)

        if not self.get_data_writing_enabled():
            logger.debug("Skipping data write because data writing is disabled")
            return row_values

        if not self._data_header_written:
            self.write_header_to_output_file()

        # TODO: output format from cfg (as in print monitor)
        with open(self.output_file, "a", encoding="utf-8") as file:
            line = "\t".join(self._format_scan_value(value) for value in row_values) + "\n"
            logger.debug("Save line to file: %s", line)
            file.write(line)

        return row_values

    def _last_point_data_headers(self) -> Tuple[List[str], Set[str]]:
        """Return ordered value headers and the detector-header set.

        Detector names are also returned as a set because timestamp-prefix
        selection runs once for every acquired value in this hot path.
        """
        data_headers = [
            pv.pvname for pv in getattr(self, "detector_pvs", [])
        ]
        detector_names = set(data_headers)
        for plugin in getattr(self, "plugins", []) or []:
            try:
                data_headers.extend(plugin.get_headers(False))
            except Exception:  # noqa: BLE001
                logger.error(
                    "Failed to get headers from plugin %s",
                    getattr(plugin, "name", plugin),
                )

        return data_headers, detector_names

    @staticmethod
    def _resolve_last_point_header(
        index: int,
        item: Any,
        data_headers: List[str],
    ) -> Optional[str]:
        """Resolve the configured or metadata-provided header for one value."""
        header = data_headers[index] if index < len(data_headers) else None
        if header or not isinstance(item, dict):
            return header
        return item.get("pvname") or item.get("name")

    @staticmethod
    def _last_point_timestamp_header(
        header: str,
        detector_names: Set[str],
    ) -> str:
        prefix = "TS-ISO8601-" if header in detector_names else "TS-"
        return prefix + header

    def _cache_last_point_item(
        self,
        last: Dict[str, Any],
        index: int,
        header: Optional[str],
        item: Any,
    ) -> None:
        """Store one acquired item under its resolved or fallback header."""
        if not header:
            last[f"col{index}"] = item
            return

        last[header] = item

    def _cache_last_point_timestamp(
        self,
        last: Dict[str, Any],
        header: str,
        item: Dict[str, Any],
        detector_names: Set[str],
    ) -> None:
        """Store the timestamp associated with one metadata-bearing value."""
        timestamp_header = self._last_point_timestamp_header(
            header,
            detector_names,
        )
        last[timestamp_header] = self._timestamp_to_iso(item.get("timestamp"))

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

        provider_headers = self._get_data_column_headers(include_timestamps)
        if provider_headers:
            self._update_data_column_provider_cache(last, include_timestamps)
        last["TS-ISO8601"] = line_ts_iso

        data_headers, detector_names = self._last_point_data_headers()
        for index, item in enumerate(values or []):
            header = self._resolve_last_point_header(index, item, data_headers)
            self._cache_last_point_item(
                last,
                index,
                header,
                item,
            )
            if include_timestamps and header and isinstance(item, dict):
                self._cache_last_point_timestamp(
                    last,
                    header,
                    item,
                    detector_names,
                )

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
        if self.output_file is None:
            logger.info("No scan data file exists")
            return None
        data_loader = DataLoader(self.output_file, data_dir=self.data_dir)
        return data_loader.load_data()

    def write_header_to_output_file(self):
        """
        Open the output file and write the headers.

        Returns:
            file: The opened file object.
        """
        if not self.get_data_writing_enabled():
            logger.debug("Skipping header write because data writing is disabled")
            return 

        if self._data_header_written:
            return

        output_file = self._ensure_output_file_exists()
        if output_file is None:
            return

        headers = self.build_output_headers(self.include_timestamps)

        with open(output_file, "w", encoding="utf-8") as file:
            file.write("\t".join(headers) + "\n")
        self._data_header_written = True
        return

    def get_stop_pv(self):
        """ 
        Read stop PV and reset it if triggered (value == 1).
        Returns the current PV value or None on failure.  
        """
        value = None
        if self.stop_pv:
            try:
                value = self.stop_pv.get()
                logger.info("Scan stop PV value received: %s", value)
            except Exception as e: # noqa: BLE001
                logger.error(
                    "Failed to get stop PV %s: %s",
                    self.stop_pv.pvname,
                    e,
                )
            if value == 1:
                try:
                    self.stop_pv.put(0)
                except Exception as e: # noqa: BLE001
                    logger.error(
                        "Failed to reset stop PV %s: %s",
                        self.stop_pv.pvname,
                        e,
                    )
        return value
    
    def _start_plugins(self) -> None:
        """Run plugin start hooks.

        Existing plugins are backward compatible because ``ScanPlugin.on_start``
        is a no-op by default. Async plugins can use this hook for debug output
        or delayed initialization.
        """
        for plugin in getattr(self, "plugins", []) or []:
            try:
                logger.debug("Starting plugin %s", getattr(plugin, "name", plugin))
                plugin.on_start()
            except Exception:
                logger.exception("Failed to start plugin %s", getattr(plugin, "name", plugin))

    def _end_plugins(self) -> None:
        """Run plugin end hooks."""
        for plugin in getattr(self, "plugins", []) or []:
            try:
                logger.debug("Ending plugin %s", getattr(plugin, "name", plugin))
                plugin.on_end()
            except Exception:
                logger.exception("Failed to end plugin %s", getattr(plugin, "name", plugin))

    def _close_plugins(self) -> None:
        """Close plugin-owned resources without requiring old plugins to change."""
        for plugin in getattr(self, "plugins", []) or []:
            close = getattr(plugin, "close", None)
            if not callable(close):
                continue
            try:
                logger.debug("Closing plugin %s", getattr(plugin, "name", plugin))
                close()
            except Exception:
                logger.exception("Failed to close plugin %s", getattr(plugin, "name", plugin))

    def _run_cleanup_step(
        self,
        label: str,
        cleanup: Callable[[], Any],
    ) -> None:
        """Run one cleanup operation without preventing later cleanup steps."""
        try:
            with self._time_block(label):
                cleanup()
        except Exception:
            logger.exception("Error during scan cleanup step '%s'", label)

    def _collect_plugin_point_data(
        self,
        index: int,
        position: Any,
    ) -> List[Any]:
        """Run point plugins and expose each result to subsequent plugins."""
        plugin_values: List[Any] = []
        for plugin in self.plugins:
            data = plugin.on_scan_point(index, position)
            plugin_values.extend(data)
            self.extend_current_row_cache(plugin.get_headers(False), data)
        return plugin_values

    def _move_scan_step(
        self,
        positions: Dict[str, List[Any]],
        index: int,
    ) -> bool:
        """Issue all actuator moves for one scan step.

        Return ``False`` if a stop was requested before all moves were issued.
        """
        self._daq_is_on = False
        for name, actuator in self.actuators.items():
            if name not in positions:
                continue

            target = positions[name][index]
            if self._stop_requested.is_set():
                logger.info("Stop requested—skipping remaining move commands.")
                return False

            logger.info("[%s] moving to %s", name, target)
            actuator.move(target)

        return not self._stop_requested.is_set()

    def _acquire_scan_point(
        self,
        index: int,
        position: Any,
        monitor: Optional[BaseMonitor],
    ) -> bool:
        """Acquire, process, save, and publish one scan point.

        Return ``False`` if acquisition was interrupted during integration.
        """
        self._reset_data_column_provider_windows()
        self._daq_is_on = True

        with self._time_block("triggers:on_point", idx=index):
            self._fire_triggers("on_point")

        if self.integration_time > 0.0:
            logger.info("DAQ for integration_time = %s", self.integration_time)
            if self._stop_requested.wait(self.integration_time):
                logger.info("Stop requested during integration time")
                return False
        else:
            logger.info("integration_time = %s", self.integration_time)

        with self._time_block("read_detectors", idx=index):
            values = self.read_detectors()
        self.update_current_row_cache(
            idx=index,
            pos=position,
            values=values,
        )

        with self._time_block("plugins", idx=index):
            plugin_values = self._collect_plugin_point_data(index, position)

        values = values + plugin_values
        with self._time_block("write:data", idx=index):
            monitor_values = self.save_to_file(
                position,
                values,
                self.include_timestamps,
            )

        self._position = position
        with self._time_block("monitor:update", idx=index):
            if monitor is not None:
                logger.debug("Monitor values: %s", monitor_values)
                monitor.update(monitor_values)

        return True

    def scan(self, positions, monitor: BaseMonitor = None):
        """
        Parallel multi-actuator scan:
         1. pad all position lists to equal length
         2. optionally prepend an overshoot point (if any backlash>0)
         3. broadcast moves, wait in parallel, then read & save (skipping the overshoot)
        """
        ensure_ca_context()
        self.busyflag = True
        self._stop_requested.clear()
        try:
            self.write_header_to_output_file()
            self._start_plugins()
            self._start_subscriptions()
            logger.debug("Actuators: %s", list(self.actuators))
            logger.debug("Requested positions: %s", positions)
            self._start_metadata_monitor()

            # prepare new_positions and tell us if we added an overshoot step
            new_positions, overshoot_applied = self._prepare_positions(positions)
            if not new_positions:
                logger.warning("No valid actuators with positions—nothing to scan.")
                self.busyflag = False
                return

            # how many total steps (includes overshoot if applied)
            step_count = len(next(iter(new_positions.values())))
            first_actuator = next(iter(new_positions))

            self._fire_triggers("before")
            for index in range(step_count):
                if self._stop_requested.is_set():
                    logger.info("Stop requested—aborting scan before step %d.", index)
                    break

                if not self._move_scan_step(new_positions, index):
                    break

                # 2) wait for all in parallel
                self._parallel_wait(
                    {name: self.actuators[name] for name in new_positions},
                    {name: new_positions[name][index] for name in new_positions}
                )

                if self._stop_requested.is_set():
                    logger.info("Stop requested—aborting scan after actuator wait.")
                    break

                # 3) skip detector‐read on the overshoot step
                if overshoot_applied and index == 0:
                    continue

                # 4) read detectors & save & monitor
                position = new_positions[first_actuator][index]
                if not self._acquire_scan_point(index, position, monitor):
                    break

                # 5) abort if needed
                if self.get_stop_pv() == 1:
                    logger.info("Stop PV triggered—aborting scan.")
                    break

            self._fire_triggers("after")
            logger.info("Scan complete for all actuators.")
        
        finally:
            self._daq_is_on = False
            self._run_cleanup_step("plugins:stop", self._end_plugins)
            self._run_cleanup_step("plugins:close", self._close_plugins)
            self._run_cleanup_step("metadata:stop", self._stop_metadata_monitor)
            self._run_cleanup_step("subscriptions:stop", self._stop_subscriptions)
            if monitor is not None:
                self._run_cleanup_step("monitor:close", monitor.close)
            self.busyflag = False
            self._run_cleanup_step("performance:report", self._perf_report)
    
    def _execute_standard(self, positions):
        if self.get_data_writing_enabled():
            with self._time_block("manifest:append"):
                self.append_to_manifest()
        else:
            logger.debug("Data writer disabled, not added to manifest")

        monitor = create_monitor(self.cfg)
        monitor_headers = self.build_output_headers(self.include_timestamps)
        if monitor is not None:
            logger.debug("Starting monitor")
            monitor.start(monitor_headers, headers=monitor_headers)

        scan_errors: List[Exception] = []

        def _run_scan() -> None:
            ensure_ca_context()
            try:
                self.scan(positions, monitor)
            except Exception as exc: # noqa: BLE001
                scan_errors.append(exc)
        scan_thread = threading.Thread(target=_run_scan, name=f"{self.__class__.__name__}-worker")
        logger.info(f"Starting {self.__class__.__name__}.")
        scan_thread.start()
        if monitor is not None:
            monitor.loop()
        scan_thread.join()

        if scan_errors:
            # Pass failures to upper layer. 
            raise scan_errors[0]
        logger.info(f"{self.__class__.__name__} scan complete.")

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
        def _wait_one(name, act):
            try:
                return act.wait_until_done(targets[name], self._stop_requested)
            except TypeError:
                return act.wait_until_done(targets[name])

        with ThreadPoolExecutor(max_workers=len(acts)) as exe:
            futures = {
                exe.submit(_wait_one, name, act): name
                for name, act in acts.items()
            }

            pending = set(futures)
            while pending:
                done, pending = wait(
                    pending,
                    timeout=0.1,
                    return_when=FIRST_COMPLETED,
                )

                for fut in done:
                    name = futures[fut]
                    exc = fut.exception()

                    if exc is not None:
                        logger.error("[%s] actuator wait failed: %s", name, exc)

                if self._stop_requested.is_set():
                    logger.info("Stop requested—waiting for actuator wait workers to exit.")

    def stop(self) -> None:
        """Request scan stop and best-effort stop all configured actuators."""
        logger.info("Stop requested for %s", self.__class__.__name__)

        try:
            self._stop_requested.set()
        except Exception:
            logger.debug("Failed to set scan stop event", exc_info=True)

        self._daq_is_on = False

        actuators = getattr(self, "actuators", {}) or {}
        for name, actuator in actuators.items():
            try:
                logger.info("Stopping actuator '%s'", name)
                actuator.stop()
            except Exception:
                logger.exception("Failed to stop actuator '%s'", name)

        try:
            self.sync_controller.wake()
            with self._tick_cond:
                self._tick_seq += 1
                self._tick_cond.notify_all()
        except Exception:
            logger.debug("Failed to wake scan wait condition", exc_info=True)

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

    def append_to_manifest(self, scan_type: str | None = None) -> None:
        """
        Append scan configuration to the active manifest.
        Args:
            scan_type: Optional explicit scan type (preferred over class name)
            metadata: Optional extra metadata dict
        """
        try:
            writer = ManifestWriter.from_active()
            if writer is None:
                return

            writer.append_scan_config(
                config=self.cfg,
                scan_type=scan_type or getattr(self, "scan_type", self.__class__.__name__),
                path=self.data_dir,
                data_file=self.output_file,
                metadata_file=self._metadata_out,
                mode=self._manifest_mode 
            )

        except Exception:
            logger.exception("Failed to append scan to manifest")
    
    # -------------------- role callback default handlers --------------------
    
    def _on_status_event(self, ev: PvEvent, _subscription: SubscriptionConfig) -> None:
        self._last_status = ev
        logger.debug("[status] %s=%r", ev.pvname, ev.value)

    def _on_heartbeat_event(self, ev: PvEvent, _subscription: SubscriptionConfig) -> None:
        self._last_heartbeat = ev
        with self._tick_cond:
            self._tick_seq += 1
            self._tick_cond.notify_all()
        logger.debug("[heartbeat] %s=%r (seq=%d)", ev.pvname, ev.value, self._tick_seq)
    
    def _on_stop_event(self, ev: PvEvent, _subscription: SubscriptionConfig) -> None:
        """
        Immediate stop trigger. Stops actuators best-effort and wakes the loop.
        """
        logger.info("[stop] %s=%r -> stopping scan", ev.pvname, ev.value)
        if self.busyflag == True:
            self._stop_requested.set()
            self.sync_controller.wake()
            with self._tick_cond:
                self._tick_cond.notify_all()
            try:
                for act in self.actuators.values():
                    act.stop()
            except Exception:
                logger.exception("Error while stopping actuators on stop event")

    def _wait_for_tick_or_timeout(self, timeout_s: float) -> bool:
        """
        Helper referring to _on_heartbeat_event and _on_status_event handlers
        Wait until:
          - a heartbeat tick arrives (returns True), or
          - timeout occurs (returns False), or
          - stop is requested (returns False).
        Example usage in scan thread or plugin threads:
                yaml: # configure what event should call _on_heartbeat_event()
                    subscriptions:
                      - name: daq_heartbeat
                        role: heartbeat
                        pv: ${IOC_MONO}:DAQ:HEARTBEAT
                py:
                self._wait_for_tick_or_timeout(self.sampletime)
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

    def _on_trigger_event(self, ev: PvEvent, _subscription: SubscriptionConfig) -> None:
        # Return immediately; do not call put() here
        self._trigger_q.put(ev)

    def _trigger_worker_loop(self) -> None:
        while not self._trigger_worker_stop.is_set():
            ev = self._trigger_q.get()
            try:
                self._fire_triggers("monitor")
            except Exception:
                logger.exception(
                    "WORKER: Failed to fire monitor triggers (ev=%s)",
                    ev,
                )

    def _on_plugin_event(self, ev: PvEvent, _subscription: SubscriptionConfig) -> None:
        """
        If the PV emits a value, plugins are triggered.
        PvEvent data provided for the plugin hook.
        """
        self._plugin_q.put(ev)
    
    def _on_sync_event(self, ev: PvEvent, subscription: SubscriptionConfig) -> None:
        """
        Record sync events for the SyncController. 
        The primary actuator RBV-style sync source updates self._position if defined as sync source.
        Example config yaml:
            subscriptions:
              - name: energy_sync
                role: sync
                actuator: energy
                source: rbv
                timeout: 1.0
        """
        self._last_sync = ev
        self.sync_controller.note_event(subscription.name)

        if self._is_position_sync_subscription(subscription):
            try:
                self._position = float(ev.value)
            except (TypeError, ValueError):
                self._position = ev.value
            self._position_sync_subscription_set = True

        logger.debug("[sync] %s=%r -> _position=%r (source=%r, sub=%s)", ev.pvname, ev.value, self._position, ev.source, subscription.name)


    def _plugin_worker_loop(self) -> None:
        while not self._plugin_worker_stop.is_set():
            ev = self._plugin_q.get()

            for plugin in self.plugins:
                try:
                    plugin.on_monitor(ev)
                except Exception:
                    logger.exception( "Plugin '%s' failed handling monitor event %s",
                        getattr(plugin, "name", type(plugin).__name__), ev.pvname)
