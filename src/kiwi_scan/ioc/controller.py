# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

""" Pure-Python controller for the generic kiwi-scan IOC API. No deps to softioc"""

from __future__ import annotations

import copy
import logging
import os
import threading
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from kiwi_scan.data.manifestwriter import ManifestWriter
from kiwi_scan.datamodels import ScanConfig, ScanDimension
from kiwi_scan.scan.registry import get_available_scan_types
from kiwi_scan.scan.tools import (
    create_scan_with_config,
    get_scan_config_dir,
    load_scan_configs,
)
from kiwi_scan.yaml_loader import yaml_loader

from .datamodels import ScanIOCStatus

logger = logging.getLogger(__name__)


def _default_scan_factory(scantype: str, config: ScanConfig, data_dir: Optional[str] = None) -> Any:
    """
    Real scan factory.
    For unit testing this can be replaced by a fake scan factory
    """
    return create_scan_with_config(scantype=scantype, config=config, data_dir=data_dir)


def default_config_dir() -> Optional[str]:
    """Return KIWI_SCAN_CONFIG_DIR or kiwi-scan's packaged config directory."""
    env_dir = os.environ.get("KIWI_SCAN_CONFIG_DIR")
    if env_dir:
        logger.debug("Using KIWI_SCAN_CONFIG_DIR=%s", env_dir)
        return env_dir
    config_dir = get_scan_config_dir()
    logger.debug("Resolved default kiwi-scan config directory: %s", config_dir)
    return config_dir

def _default_config_loader(config_dir: str, replacements: Mapping[str, str]) -> Dict[str, ScanConfig]:
    """ 
    Production config loader, fake config loader in unit tests
    """
    return load_scan_configs(config_dir, replacements)

def _load_scan_config_from_file(config_file: str, replacements: Mapping[str, str]) -> ScanConfig:
    data = yaml_loader(config_file, dict(replacements or {}))
    return ScanConfig.from_dict(data)


def _describe_dimensions(dimensions: Sequence[ScanDimension]) -> str:
    parts = []
    for dim in dimensions:
        parts.append(
            "{}:start={},stop={},steps={},velocity={}".format(
                dim.actuator,
                dim.start,
                dim.stop,
                dim.steps,
                getattr(dim, "velocity", 0.0),
            )
        )
    return "; ".join(parts) or "<none>"


class ScanIOCController:
    """
    The controller manages scan execution based on a template :class:`ScanConfig`.
    For each scan run, it creates a copy of the base configuration, updates it with 
    scan dimensions provided by the IOC, and delegates execution to the public scan API.

    This controller is used by the EPICS interface defined in
    ``generic_scan_ioc.py``.
    TODO: data interface using kiwi manifests and data loaders for pv_accass
    """

    def __init__(
        self,
        *,
        scan_type: str,
        config_name: Optional[str] = None,
        config_file: Optional[str] = None,
        config_dir: Optional[str] = None,
        data_dir: Optional[str] = None,
        replacements: Optional[Mapping[str, str]] = None,
        scan_factory: Optional[Callable[[str, ScanConfig, Optional[str]], Any]] = None,
        config_loader: Optional[Callable[[str, Mapping[str, str]], Dict[str, ScanConfig]]] = None,
    ) -> None:
        if bool(config_name) == bool(config_file):
            raise ValueError("exactly one of config_name or config_file must be provided")

        self.scan_type = scan_type
        self.config_name = config_name
        self.config_file = config_file
        self.config_dir = config_dir or default_config_dir()
        self.data_dir = data_dir
        self.replacements: Dict[str, str] = dict(replacements or {})
        
        self._scan_factory = scan_factory or _default_scan_factory
        self._config_loader = config_loader or _default_config_loader

        self._lock = threading.RLock()
        self.scan: Any = None
        self.status = ScanIOCStatus.IDLE
        self.message = "idle"

        logger.info(
            "Initializing ScanIOCController scan_type=%s config_name=%s config_file=%s config_dir=%s data_dir=%s replacements=%s",
            self.scan_type, self.config_name, self.config_file, self.config_dir, self.data_dir, sorted(self.replacements.keys()),
        )
        self.base_config = self._load_base_config()
        self._data_writing_enabled_default = bool(
            getattr(self.base_config, "data_writing_enabled", True)
        )
        self._log_loaded_config("Loaded IOC base config")

    @staticmethod
    def _error_message(exc: BaseException) -> str:
        """ Returns single-line message for string record. """
        message = str(exc).replace("\n", " ").replace("\r", " ").strip()
        return message or type(exc).__name__

    def set_message(self, message: Any) -> None:
        """ Update  message """
        text = str(message).replace("\n", " ").replace("\r", " ").strip()
        with self._lock:
            self.message = text

    def set_error(self, exc: BaseException) -> None:
        """ Set IOC error state until the next start. """
        with self._lock:
            self.status = ScanIOCStatus.ERROR
            self.message = self._error_message(exc)

    # -------------------------------------------------------- Configuration
    
    def create_new_manifest(self) -> str:
        """ Create and select a new active manifest for subsequent scans when IOC is idle """

        with self._lock:
            self._require_idle_unlocked()
            directory = self.data_dir or os.environ.get("KIWI_SCAN_DATA_DIR")
            path = ManifestWriter.newmanifest(directory=directory)
            self.message = f"new manifest: {os.path.basename(path)}"

        logger.info("Created and selected new active manifest: %s", path)
        return path

    def get_manifest_file(self) -> str:
        """ Return manifest path if available. """
        with self._lock:
            return ManifestWriter.get_active_manifest() or ""

    def _load_named_config(self, config_name: str) -> ScanConfig:
        """ Load one named config from ``config_dir``."""
        if not self.config_dir:
            raise FileNotFoundError(
                "no config_dir configured and KIWI_SCAN_CONFIG_DIR is not set"
            )

        logger.info(
            "Loading IOC scan config %r from directory: %s",
            config_name,
            self.config_dir,
        )
        configs = self._config_loader(self.config_dir, self.replacements)
        logger.debug("Available IOC scan configs: %s", sorted(configs.keys()))
        try:
            return configs[str(config_name)]
        except KeyError as exc:
            available = ", ".join(sorted(configs)) or "<none>"
            raise KeyError(
                f"Scan config {config_name!r} not found in {self.config_dir!r}. Available configs: {available}"
            ) from exc

    def _load_base_config(self) -> ScanConfig:
        """ Load config from given file """
        if self.config_file:
            path = os.path.abspath(os.path.expanduser(self.config_file))
            logger.info("Loading IOC scan config from file: %s", path)
            if not os.path.isfile(path):
                raise FileNotFoundError(f"config file not found: {path}")
            config = _load_scan_config_from_file(path, self.replacements)
            logger.debug("Loaded config file %s", path)
            return config

        return self._load_named_config(str(self.config_name))

    def _log_loaded_config(self, message: str) -> None:
        logger.info(
            "%s: actuators=%d detector_pvs=%d scan_dimensions=%d data_writing_enabled=%s",
            message,
            len(getattr(self.base_config, "actuators", {}) or {}),
            len(getattr(self.base_config, "detector_pvs", []) or []),
            len(getattr(self.base_config, "scan_dimensions", []) or []),
            self._data_writing_enabled_default,
        )

    def _reload_base_config_unlocked(
        self,
        *,
        preserve_data_writing_default: bool,
    ) -> None:
        """
        Reload the YAML-backed scan template.
        The caller must hold ``self._lock``.
        Preserves the current IOC DataWritingEnabled setting when preserve_data_writing_default == False
        for PV is a runtime control.
        """
        current_data_writing_default = bool(self._data_writing_enabled_default)
        self.base_config = self._load_base_config()

        if preserve_data_writing_default:
            self._data_writing_enabled_default = current_data_writing_default
        else:
            self._data_writing_enabled_default = bool(
                getattr(self.base_config, "data_writing_enabled", True)
            )

    def _require_idle_unlocked(self) -> None:
        """ Reject runtime configuration changes while a scan is active. """
        if self.scan is not None or self.status in (
            ScanIOCStatus.INITIALIZING,
            ScanIOCStatus.RUNNING,
        ):
            raise RuntimeError(
                "cannot change scan configuration while a scan is active"
            )

    def reload_config(self) -> None:
        """ Reload the base configuration while no scan is active. """
        with self._lock:
            self._require_idle_unlocked()
            logger.info("Reloading IOC base config")
            self._reload_base_config_unlocked(preserve_data_writing_default=False)
            self.message = "config reloaded"
            self._log_loaded_config("IOC base config reloaded")

    def get_config_name(self) -> str:
        """ Return the selected named config, or an empty string in file mode. """
        with self._lock:
            return str(self.config_name or "")

    def set_config_name(self, config_name: str) -> None:
        """ 
        Select a named config for subsequent scans.

        Runtime switching is limited to configs discoverable in
        ``config_dir``. An IOC started with ``config_file`` remains in fixed file mode.
        TODO: search in config_file path as well for switching back/forth from config-file
        """
        config_name = str(config_name).strip()
        if not config_name:
            raise ValueError("config name must not be empty")

        with self._lock:
            self._require_idle_unlocked()
            if self.config_file is not None:
                raise RuntimeError(
                    "runtime config switching is unavailable when the IOC "
                    "was started with config_file"
                )

            scan_config_loaded = self._load_named_config(config_name)

            # Commit only after loading and validation succeeded. This must not fail. 
            # Keep the runtime DataWritingEnabled setting unchanged.
            self.config_name = config_name
            self.base_config = scan_config_loaded
            self.message = f"config selected: {config_name}"
            self._log_loaded_config("Selected IOC base config")

    def get_scan_type(self) -> str:
        """ Return the scan type selected for subsequent scans. """
        with self._lock:
            return str(self.scan_type or "")

    def set_scan_type(self, scan_type: str) -> None:
        """ Select a registered scan type for subsequent scans. """
        scan_type = str(scan_type).strip()
        if not scan_type:
            raise ValueError("scan type must not be empty")

        available = set(get_available_scan_types())
        if available and scan_type not in available:
            raise ValueError(
                "unknown scan type {!r}; available types: {}".format(scan_type, ", ".join(sorted(available)))
            )

        with self._lock:
            self._require_idle_unlocked()
            self.scan_type = scan_type
            self.message = f"scan type selected: {scan_type}"

    def dimension_defaults(self) -> Tuple[str, float, float, int, float]:
        """ Return default actuator/start/stop/steps/velocity values for IOC PVs. """
        dims = getattr(self.base_config, "scan_dimensions", None) or []
        if dims:
            dim = dims[0]
            defaults = (
                str(dim.actuator),
                float(dim.start),
                float(dim.stop),
                int(dim.steps),
                float(getattr(dim, "velocity", 0.0) or 0.0),
            )
            logger.debug("Using scan_dimension defaults for IOC records: %s", defaults)
            return defaults

        actuators = getattr(self.base_config, "actuators", None) or {}
        actuator = next(iter(actuators.keys()), "")
        defaults = (str(actuator), 0.0, 0.0, 1, 0.0)
        logger.debug("Using fallback actuator defaults for IOC records: %s", defaults)
        return defaults

    @staticmethod
    def make_dimension( #TODO
        *,
        actuator: str,
        start: Any,
        stop: Any,
        steps: Any,
        velocity: Any = 0.0,
    ) -> ScanDimension:
        actuator = str(actuator).strip()
        if not actuator:
            raise ValueError("Actuator must not be empty")
        parsed_steps = int(steps)
        if parsed_steps < 1:
            raise ValueError(f"Steps must be >= 1, got {steps!r}")
        parsed_velocity = float(velocity or 0.0)
        if parsed_velocity < 0:
            raise ValueError(f"Velocity must be >= 0, got {velocity!r}")
        return ScanDimension(
            actuator=actuator,
            start=float(start),
            stop=float(stop),
            steps=parsed_steps,
            velocity=parsed_velocity,
        )

    # --------------------------------------------------------------- Scan 

    def create_scan(self, dimensions: Sequence[ScanDimension]) -> Any:
        if not dimensions:
            raise ValueError("at least one ScanDimension is required")

        logger.info("Creating scan object type=%s dimensions=%s", self.scan_type, _describe_dimensions(dimensions))
        logger.info("Reloading IOC scan config before run")
        self._reload_base_config_unlocked(preserve_data_writing_default=True)
        self._log_loaded_config("Reloaded IOC scan config for run")

        cfg = copy.deepcopy(self.base_config) # can be changed
        cfg.scan_dimensions = list(dimensions)
        cfg.data_writing_enabled = bool(self._data_writing_enabled_default)

        scan = self._scan_factory(self.scan_type, cfg, self.data_dir)
        if scan is None:
            raise RuntimeError(f"Failed to create scan object for type {self.scan_type!r}")
        logger.info("Created scan object %s", type(scan).__name__)
        return scan

    def start(self, dimensions: Sequence[ScanDimension]) -> Any:
        """
        Create the scan object for a new run.
        The scan is executed in a worker thread to avoid blocking the IOC main task.
        """
        with self._lock:
            if self.scan is not None and bool(getattr(self.scan, "busy", False)):
                logger.warning("Start rejected: scan object is busy")
                raise RuntimeError("scan already running")
            if self.scan is not None and self.status in (
                ScanIOCStatus.INITIALIZING,
                ScanIOCStatus.RUNNING,
            ):
                logger.warning("Start rejected: scan is already active with status=%s", self.status.name)
                raise RuntimeError("scan already active")

            logger.info("Starting IOC scan preparation")
            self.status = ScanIOCStatus.INITIALIZING
            self.message = "initializing"
            try:
                self.scan = self.create_scan(dimensions)
            except Exception as exc:
                self.status = ScanIOCStatus.ERROR
                self.message = self._error_message(exc)
                self.scan = None
                logger.exception("Failed to prepare IOC scan")
                # re raise here
                raise
            logger.info("IOC scan prepared")
            return self.scan

    def execute_current_scan(self) -> None:
        """Execute the current scan synchronously."""
        with self._lock:
            scan = self.scan
            if scan is None:
                raise RuntimeError("no scan has been created")
            self.status = ScanIOCStatus.RUNNING
            self.message = "running"
            logger.info("Executing IOC scan %s", type(scan).__name__)

        try:
            scan.execute()
        except Exception as exc:
            with self._lock:
                self.status = ScanIOCStatus.ERROR
                self.message = self._error_message(exc)
                self.scan = None
            logger.exception("IOC scan execution failed")
            raise
        else:
            with self._lock:
                self.status = ScanIOCStatus.IDLE
                self.message = "idle"
                self.scan = None
            logger.info("IOC scan execution completed")

    def run_scan(self, dimensions: Sequence[ScanDimension]) -> None:
        """ Execute scan. This should be executed in its own task (loop.create_task). Obsolete helper function """
        self.start(dimensions)
        self.execute_current_scan()

    def stop(self) -> None:
        with self._lock:
            scan = self.scan
        if scan is None:
            logger.info("Stop requested but no scan is active")
            self.message = "no active scan"
            return
        try:
            logger.info("Stop requested for IOC scan")
            scan.stop()
            self.message = "stop requested"
        except Exception as exc:
            logger.exception("Failed to stop scan")
            with self._lock:
                self.status = ScanIOCStatus.ERROR
                self.message = self._error_message(exc)
            raise

    # --------------------------------------------------------- Safe public scan API wrappers

    def get_busy(self) -> bool:
        with self._lock:
            scan = self.scan
        return bool(scan is not None and getattr(scan, "busy", False))

    def get_position(self, default: float = float("nan")) -> float:
        with self._lock:
            scan = self.scan
        if scan is None:
            return default
        try:
            value = getattr(scan, "position", default)
            if value is None:
                return default
            return float(value)
        except Exception:
            logger.debug("Failed to read scan position", exc_info=True)
            return default

    def get_value(self, key: str, default: Any = None) -> Any:
        with self._lock:
            scan = self.scan
        if scan is None:
            return default
        try:
            return scan.get_value(key, default=default)
        except Exception:
            logger.debug("Failed to read scan value %r", key, exc_info=True)
            return default

    def get_output_file(self) -> str:
        with self._lock:
            scan = self.scan
        if scan is None:
            return ""
        try:
            return scan.get_output_file() or ""
        except Exception:
            logger.debug("Failed to read output file", exc_info=True)
            return ""

    def set_data_writing_enabled(self, enabled: bool) -> None:
        enabled = bool(enabled)
        with self._lock:
            self._data_writing_enabled_default = enabled
            scan = self.scan
        if scan is not None:
            logger.info("Setting active scan data_writing_enabled=%s", enabled)
            scan.set_data_writing_enabled(enabled)
        else:
            logger.info("Setting default IOC data_writing_enabled=%s", enabled)
        self.message = "data writing %s" % ("enabled" if enabled else "disabled")

    def get_data_writing_enabled(self) -> bool:
        with self._lock:
            scan = self.scan
            default = self._data_writing_enabled_default
        if scan is None:
            return bool(default)
        try:
            return bool(scan.get_data_writing_enabled())
        except Exception:
            logger.debug("Failed to read data writing flag", exc_info=True)
            return bool(default)
