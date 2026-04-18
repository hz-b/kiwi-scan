# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import logging
import time
import os
import weakref

from kiwi_scan.plugin.registry import PluginConfig
from kiwi_scan.dataloader import get_kiwi_data_dir_from_environ
from kiwi_scan.actuator.single import PvEvent 

def wrap_values(values: List[Any]) -> List[Dict[str, Any]]:
    """
    Normalize plugin output values into the record format expected by the scan writer.

    This method should take the raw values produced by the plugin, align them
    with the expanded headers, and return a serializable list that can be
    written directly into the scan row. If `timestamps` is enabled, include the
    corresponding timestamp values in the correct positions.
    """
    result = []
    now = time.time()  # current time in seconds as float
    sec = int(now)
    nsec = int((now - sec) * 1e9)

    for v in values:
        result.append({
            'value': v,
            'timestamp': now,
            'posixseconds': sec,
            'nanoseconds': nsec
        })
    return result

class ScanPlugin(ABC):
    def __init__(self, 
                 name: str,
                 parameters: Optional[Dict[str, Any]] = None,
                 scan: Optional["BaseScan"] = None):

        self.name = name
        self.parameters = parameters or {}
        self.logger = logging.getLogger(f"ScanPlugin.{self.name}")
        kiwi_data_dir = get_kiwi_data_dir_from_environ()
        plugin_log_dir = 'plugin_log'
        self.DEFAULT_LOG_FILE = 'plugin.log'
        if kiwi_data_dir is not None:
            self.log_dir = os.path.normpath(os.path.join(kiwi_data_dir, plugin_log_dir))
        else: 
            current_file_dir = os.path.dirname(os.path.abspath(__file__))
            self.log_dir =  os.path.normpath(os.path.join(current_file_dir, '..', '..', '..', plugin_log_dir))
        # Store reference to BaseScan from scan.base preventing reference cycles
        self.scan: Optional["BaseScan"] = (
            weakref.proxy(scan) if scan is not None else None
        )

    def _init_logging(self) -> None:
        """Configure plugin-specific file logging."""
        cfg = getattr(self.scan, "cfg", None)
        level = self.parameters.get(
            "log_level", getattr(cfg, "logging_level", logging.INFO)
        )
        self.logger.setLevel(level)

        # BUGFIX: cannot access local variable 'filepath' where it is not associated with a value: 
        # Always compute filepath
        filepath = os.path.join(
            self.log_dir,
            self.parameters.get("log_file", self.DEFAULT_LOG_FILE),
        )

        # Ensure log directory exists
        os.makedirs(self.log_dir, exist_ok=True)

        # Only add handler once
        if not self.logger.handlers:
            handler = logging.FileHandler(filepath)
            handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            ))
            self.logger.addHandler(handler)

        self.logger.debug("Logging initialized at level %s to %s", level, filepath)
    
    def expand_headers(self, hdrs: List[str], timestamps: bool = False) -> List[str]:
        """
        Expand this plugin’s logical output fields into the concrete scan-file column headers.
        This should return the headers in the exact order they will appear in the output row.
        When `timestamps` is true, append any matching timestamp columns using the same ordering convention as the rest of the scan output.
        """
        if timestamps:
            expanded = []
            for h in hdrs:
                expanded += [h, f"TS-{h}"]
            return expanded
        return hdrs
    
    def on_start(self) -> None:
        pass

    def pre_move(self, idx: int, pos: Dict[str, Any]) -> None:
        pass

    def post_move(self, idx: int, pos: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def get_headers(self, timestamps: bool) -> List[str]:
        """
        Subclasses must return a list of column header strings.
        """
        pass
    
    @abstractmethod
    def get_values(self, idx: int, pos: Dict[str, Any]) -> List[Any]:
        """Return additional data at each scan point. Data must match header."""
        pass
    
    def on_scan_point(self, idx: int, pos: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Return column headers wrapped as [{'value': header}, ...].
        Subclasses must implement get_header_names().
        """
        return wrap_values(self.get_values(idx, pos))

    def on_end(self) -> None:
        pass

    def on_monitor(self, ev: PvEvent) -> None:
        """
        Optional hook: called on monitor updates.
        Default: Used as debug log.
        """
        if not self.logger.isEnabledFor(logging.DEBUG):
            return

        # Prefer ISO timestamp if available
        ts = ev.timestamp
        if ts is not None:
            try:
                from datetime import datetime, timezone
                ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except Exception:
                ts_str = str(ts)
        else:
            ts_str = "n/a"

        self.logger.info(
            "[MON] pv=%s value=%r ts=%s sev=%r stat=%r src=%r",
            ev.pvname,
            ev.value,
            ts_str,
            ev.severity,
            ev.status,
            ev.source,
        )

