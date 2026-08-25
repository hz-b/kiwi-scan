# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.monitor.row_format import MonitorRowFormatter, MonitorValueFormatter


class PrintMonitor(BaseMonitor):
    """stdout monitor for detector, subscription and plugin values.

    Writes a clean machine-readable data stream.

    Parameters:
      format: tsv | csv | json
      include_timestamps: bool
      include_header: bool
      float_format: str
    """

    SUPPORTED_FORMATS = MonitorRowFormatter.SUPPORTED_FORMATS
    DEFAULT_FORMAT = MonitorRowFormatter.DEFAULT_FORMAT
    DEFAULT_FLOAT_FORMAT = MonitorValueFormatter.DEFAULT_FLOAT_FORMAT

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        self.parameters = parameters or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        print_cfg = self._print_config(self.parameters)
        self._print_enabled = bool(print_cfg.pop("enabled", True))
        self._formatter_parameters = dict(print_cfg)
        self._formatter = MonitorRowFormatter(self._formatter_parameters, logger=self.logger)
        self.signal_names: List[str] = []
        self.headers: List[str] = []

        self.logger.info(
            "Initialized PrintMonitor enabled=%s format=%s include_timestamps=%s include_header=%s float_format=%s",
            self._print_enabled,
            self._formatter.format,
            self._formatter.include_timestamps,
            self._formatter.include_header,
            self._formatter.value_formatter.float_format,
        )

    @staticmethod
    def _print_config(parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Return the nested monitor.print configuration block."""
        raw = parameters.get("print", {})
        if raw is False:
            return {"enabled": False}
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise TypeError("monitor.print must be a mapping or false")
        return dict(raw)

    def start(self, signal_names: Iterable[str], headers: Optional[Iterable[str]] = None) -> None:
        self.signal_names = list(signal_names)
        self.headers = list(headers) if headers is not None else list(self.signal_names)
        self.logger.debug(
            "Starting PrintMonitor for signals=%r headers=%r enabled=%s",
            self.signal_names,
            self.headers,
            self._print_enabled,
        )

        formatter_parameters = dict(self._formatter_parameters)
        if headers is not None:
            formatter_parameters["include_timestamps"] = False
            self._formatter = MonitorRowFormatter(formatter_parameters, logger=self.logger)

        if self._print_enabled:
            self._formatter.start(self.headers)

    def update(self, vals: List[Any]) -> None:
        self.logger.debug( "PrintMonitor update received %d values for %d signals", len(vals), len(self.signal_names))
        if not self._print_enabled:
            return
        self._formatter.write(vals)

    def loop(self) -> None:
        self.logger.debug("PrintMonitor loop(): no background loop required")

    def close(self) -> None:
        self.logger.debug( "Closing PrintMonitor after writing %d rows", self._formatter.rows_written)
        self._formatter.close()
