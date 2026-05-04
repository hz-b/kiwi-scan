# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from kiwi_scan.monitor.base import BaseMonitor


class PrintMonitor(BaseMonitor):
    """stdout monitor for daetector values.

    Writes a clean machine-readable data stream.

    Parameters:
      format: tsv | csv | json
      include_timestamps: bool
      include_header: bool
      float_format: str
    """

    SUPPORTED_FORMATS = {"tsv", "csv", "json"}
    DEFAULT_FORMAT = "tsv"
    DEFAULT_FLOAT_FORMAT = ".12e"

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        self.parameters = parameters or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        # self.logger.info(f"{self.parameters}")

        self.format = str(self.parameters.get("format", self.DEFAULT_FORMAT)).lower()
        self.include_timestamps = bool(self.parameters.get("include_timestamps", False))
        self.include_header = bool(self.parameters.get("include_header", True))
        self.float_format = str(self.parameters.get("float_format", self.DEFAULT_FLOAT_FORMAT))

        self.signal_names: List[str] = []
        self._writer: Optional[csv.writer] = None
        self._out = sys.stdout
        self._rows_written = 0

        self.logger.info( "Initialized PrintMonitor format=%s include_timestamps=%s include_header=%s float_format=%s",
            self.format, self.include_timestamps, self.include_header, self.float_format)

    def start(self, signal_names: Iterable[str]) -> None:
        self.signal_names = list(signal_names)
        self._rows_written = 0

        self.logger.debug("Starting PrintMonitor for signals=%r", self.signal_names)

        if self.format not in self.SUPPORTED_FORMATS:
            self.logger.warning(
                "Unsupported PrintMonitor format requested: %r. Falling back to default format %r. Supported formats are: %s",
                self.format, self.DEFAULT_FORMAT, ", ".join(sorted(self.SUPPORTED_FORMATS)))
            self.format = self.DEFAULT_FORMAT

        if self.format in ("tsv", "csv"):
            delimiter = "\t" if self.format == "tsv" else ","
            self._writer = csv.writer(self._out, delimiter=delimiter, lineterminator="\n")
            self.logger.debug("Configured delimited PrintMonitor writer delimiter=%r", delimiter)

            if self.include_header:
                headers = self._headers()
                self.logger.debug("Writing PrintMonitor header: %r", headers)
                self._writer.writerow(headers)
                self._out.flush()
        else:
            # no separate header
            self.logger.debug("Configured PrintMonitor for JSON Lines output")

    def update(self, vals: List[Any]) -> None:
        self.logger.debug( "PrintMonitor update received %d values for %d signals",
            len(vals), len(self.signal_names))

        if len(vals) != len(self.signal_names):
            self.logger.debug(
                "Value/header count mismatch: values=%d signals=%d. " "Using available order and fallback names for extra values.",
                len(vals), len(self.signal_names))

        if self.format in ("tsv", "csv"):
            if self._writer is None:
                self.logger.error("PrintMonitor writer was not initialized before update")
                return

            row = self._row(vals)
            self.logger.debug("Writing PrintMonitor row %d: %r", self._rows_written + 1, row)
            self._writer.writerow(row)
            self._out.flush()
            self._rows_written += 1
            return

        if self.format == "json":
            obj = self._json_obj(vals)
            self.logger.debug(
                "Writing PrintMonitor JSON object %d with keys=%r",
                self._rows_written + 1,
                list(obj.keys()),
            )
            print(json.dumps(obj, ensure_ascii=False, default=str), file=self._out, flush=True)
            self._rows_written += 1
            return

        self.logger.error("Unsupported PrintMonitor format at update time: %s", self.format)

    def loop(self) -> None:
        self.logger.debug("PrintMonitor loop() called; no background loop required")
        return

    def close(self) -> None:
        # Do not print a final Python list representation: stdout is the data stream.
        self.logger.debug("Closing PrintMonitor after writing %d rows", self._rows_written)
        try:
            self._out.flush()
        except Exception:
            self.logger.info("Failed to flush PrintMonitor output stream", exc_info=True)

    def _headers(self) -> List[str]:
        headers: List[str] = []
        for name in self.signal_names:
            headers.append(name)
            if self.include_timestamps:
                headers.append(f"TS-ISO8601-{name}")
        return headers

    def _row(self, vals: List[Any]) -> List[str]:
        row: List[str] = []
        for item in vals:
            row.append(self._format_value(self._extract_value(item)))
            if self.include_timestamps:
                row.append(self._format_timestamp(self._extract_timestamp(item)))
        return row

    def _json_obj(self, vals: List[Any]) -> Dict[str, Any]:
        obj: Dict[str, Any] = {}
        for idx, item in enumerate(vals):
            name = self.signal_names[idx] if idx < len(self.signal_names) else f"col{idx}"
            obj[name] = self._extract_value(item)
            if self.include_timestamps:
                obj[f"TS-ISO8601-{name}"] = self._format_timestamp(
                    self._extract_timestamp(item)
                )
        return obj

    def _extract_value(self, item: Any) -> Any:
        if item is None:
            self.logger.debug("Encountered None monitor item; writing empty value")
            return None

        if isinstance(item, dict):
            if "value" not in item:
                self.logger.debug( "Monitor item dict has no 'value' key; item keys=%r", list(item.keys()))
            return item.get("value")

        self.logger.debug("Monitor item is not a dict; using raw item value: %r", item)
        return item

    def _extract_timestamp(self, item: Any) -> Any:
        if not isinstance(item, dict):
            return None
        return item.get("timestamp")

    def _format_value(self, value: Any) -> str:
        if value is None:
            return ""

        try:
            return format(float(value), self.float_format)
        except (TypeError, ValueError):
            self.logger.debug("Value is not float-convertible; writing as string: %r", value)
            return str(value)

    def _format_timestamp(self, ts: Any) -> str:
        if ts is None:
            return ""

        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OSError, OverflowError):
            self.logger.debug( "Timestamp is not POSIX-float-convertible; writing as string: %r", ts)
            return str(ts)

