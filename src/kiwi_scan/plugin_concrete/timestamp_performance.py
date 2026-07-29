# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional

from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import register_plugin
from kiwi_scan.tools import timestamp_to_seconds


@register_plugin("TimestampPerformancePlugin")
class TimestampPerformancePlugin(ScanPlugin):
    """
    Add timing diagnostics for detector timestamp columns for performance testing.

    - `PerfPointDeltaS`: seconds since the previous plugin call.
    - `PerfDeltaS-<PV>`: seconds since the previous timestamp of that PV.
    - `PerfAgeS-<PV>`: plugin reference time minus the PV timestamp.

    """

    TIMESTAMP_PREFIX = "TS-ISO8601-"

    def __init__(
        self,
        name: str,
        parameters: Optional[Dict[str, Any]] = None,
        scan: Optional["BaseScan"] = None,
    ) -> None:
        super().__init__(name=name, parameters=parameters, scan=scan)

        detector_pvs = list(
            getattr(getattr(scan, "cfg", None), "detector_pvs", None) or []
        )
        # Preserve configuration order while avoiding duplicate output columns.
        self._timestamp_columns = [
            self.TIMESTAMP_PREFIX + pvname
            for pvname in dict.fromkeys(str(pv) for pv in detector_pvs)
        ]

        self._previous_point_time: Optional[float] = None
        self._previous_timestamp: Dict[str, float] = {}

        self.logger.debug("Timestamp columns: %s", self._timestamp_columns)

    def get_headers(self, timestamps: bool) -> List[str]:
        headers: List[str] = ["PerfPointDeltaS"]
        for timestamp_column in self._timestamp_columns:
            source = timestamp_column[len(self.TIMESTAMP_PREFIX):]
            headers.extend(
                [
                    "PerfDeltaS-" + source,
                    "PerfAgeS-" + source,
                ]
            )
        return self.expand_headers(headers, timestamps)

    def get_values(self, idx: int, pos: Any) -> List[Any]:
        del idx, pos

        reference_time = float(time.time())
        point_delta = self._diff(reference_time, self._previous_point_time)
        self._previous_point_time = reference_time

        row = self.scan.get_current_row_cache() if self.scan is not None else {}
        values: List[float] = [point_delta]

        for timestamp_column in self._timestamp_columns:
            raw_timestamp = row.get(timestamp_column)
            timestamp = timestamp_to_seconds(raw_timestamp)
            if timestamp is None:
                self.logger.debug("No valid timestamp @ column %s: %r", timestamp_column, raw_timestamp)
                values.extend([math.nan, math.nan])
                continue

            timestamp_delta = self._diff(
                timestamp,
                self._previous_timestamp.get(timestamp_column),
            )
            timestamp_age = reference_time - timestamp
            self._previous_timestamp[timestamp_column] = timestamp

            # self.logger.debug("Timestamp performance column=%s delta=%r age=%r", timestamp_column, timestamp_delta, timestamp_age)
            values.extend([float(timestamp_delta), float(timestamp_age)])

        return values

    def on_start(self) -> None:
        """ Reset TS history """
        self._previous_point_time = None
        self._previous_timestamp.clear()

    @staticmethod
    def _diff(current: float, previous: Optional[float]) -> float:
        if previous is None:
            return math.nan
        return float(current - previous)

