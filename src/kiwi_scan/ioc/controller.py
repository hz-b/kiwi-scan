# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Pure-Python controller for the generic kiwi-scan IOC API."""

from __future__ import annotations

import copy
import logging
import os
import threading
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from kiwi_scan.datamodels import ScanConfig, ScanDimension
from kiwi_scan.yaml_loader import yaml_loader
from kiwi_scan.scan.tools import create_scan_with_config
from kiwi_scan.scan.tools import load_scan_configs, get_scan_config_dir

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
            "%s:start=%s,stop=%s,steps=%s,velocity=%s"
            % (
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
        logger.info(
            "Loaded IOC base config: actuators=%d detector_pvs=%d scan_dimensions=%d data_writing_enabled=%s",
            len(getattr(self.base_config, "actuators", {}) or {}),
            len(getattr(self.base_config, "detector_pvs", []) or []),
            len(getattr(self.base_config, "scan_dimensions", []) or []),
            self._data_writing_enabled_default,
        )

    # --------------------------------------------------------
    # Configuration

    def _load_base_config(self) -> ScanConfig:
        if self.config_file:
            path = os.path.abspath(os.path.expanduser(self.config_file))
            logger.info("Loading IOC scan config from file: %s", path)
            if not os.path.isfile(path):
                raise FileNotFoundError("config file not found: %s" % path)
            config = _load_scan_config_from_file(path, self.replacements)
            logger.debug("Loaded config file %s", path)
            return config

        if not self.config_dir:
            raise FileNotFoundError("no config_dir configured and KIWI_SCAN_CONFIG_DIR is not set")
        logger.info("Loading IOC scan config %r from directory: %s", self.config_name, self.config_dir)
        configs = self._config_loader(self.config_dir, self.replacements)
        logger.debug("Available IOC scan configs: %s", sorted(configs.keys()))
        try:
            return configs[str(self.config_name)]
        except KeyError as exc:
            available = ", ".join(sorted(configs)) or "<none>"
            raise KeyError(
                "Scan config %r not found in %r. Available configs: %s"
                % (self.config_name, self.config_dir, available)
            ) from exc

    def reload_config(self) -> None:
        """TODO: Reload the base configuration."""
        with self._lock:
            if self.get_busy():
                raise RuntimeError("cannot reload config while a scan is running")
            logger.info("Reloading IOC base config")
            self.base_config = self._load_base_config()
            self._data_writing_enabled_default = bool(
                getattr(self.base_config, "data_writing_enabled", True)
            )
            self.message = "config reloaded"
            logger.info("IOC base config reloaded")

    def dimension_defaults(self) -> Tuple[str, float, float, int, float]:
        """Return default actuator/start/stop/steps/velocity values for IOC PVs."""
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
            raise ValueError("Steps must be >= 1, got %r" % steps)
        parsed_velocity = float(velocity or 0.0)
        if parsed_velocity < 0:
            raise ValueError("Velocity must be >= 0, got %r" % velocity)
        return ScanDimension(
            actuator=actuator,
            start=float(start),
            stop=float(stop),
            steps=parsed_steps,
            velocity=parsed_velocity,
        )

    # ---------------------------------------------------------------
    # Scan 

    def create_scan(self, dimensions: Sequence[ScanDimension]) -> Any:
        if not dimensions:
            raise ValueError("at least one ScanDimension is required")

        logger.info("Creating scan object type=%s dimensions=%s", self.scan_type, _describe_dimensions(dimensions))
        cfg = copy.deepcopy(self.base_config)
        cfg.scan_dimensions = list(dimensions)
        cfg.data_writing_enabled = bool(self._data_writing_enabled_default)

        scan = self._scan_factory(self.scan_type, cfg, self.data_dir)
        if scan is None:
            raise RuntimeError("Failed to create scan object for type %r" % self.scan_type)
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
                self.message = str(exc)
                self.scan = None
                logger.exception("Failed to prepare IOC scan")
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
                self.message = str(exc)
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
        """Convenience method: create and execute one scan synchronously."""
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
                self.message = str(exc)
            raise

    # ------------------------------------------------------------------
    # Safe public scan API wrappers

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
            return "None"
        try:
            return scan.get_output_file() or "unknown"
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
