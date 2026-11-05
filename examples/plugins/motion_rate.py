# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""
MotionRatePlugin: rate of change of one scan signal, fitted in the background.

The plugin fits ``value = slope * time + offset`` over a sliding window of the
last ``window`` samples of one row key ``signal`` (for example the energy
readback PV). The time axis is the detector timestamp ``TS-<signal>`` of the
same point. The first timestamp of the window is subtracted before fitting,
so ``MotionRateOffset`` is the fitted value at the start of the window.

Usage::

    export KIWI_SCAN_PLUGIN_PATH=/path/to/kiwi-scan/examples/plugins

See ``examples/plugins/motion_rate.md`` for the YAML configuration.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

import numpy as np

from kiwi_scan.plugin.async_base import AsyncScanPlugin
from kiwi_scan.plugin.registry import register_plugin

TIMESTAMP_PREFIX = "TS-"
DEFAULT_WINDOW = 50
MIN_WINDOW = 2

STATUS_PENDING = "pending"
STATUS_READY = "ready"
STATUS_ERROR = "error"

# A snapshot handed to the worker: (newest scan index, timestamps, values).
Snapshot = Tuple[int, Tuple[float, ...], Tuple[float, ...]]


def _parse_signal(parameters: Dict[str, Any]) -> str:
    signal = parameters.get("signal")
    if not isinstance(signal, str) or not signal.strip():
        raise ValueError(
            "MotionRatePlugin parameter 'signal' is required and must be a "
            f"non-empty row key such as a detector PV name, got {signal!r}"
        )
    return signal.strip()


def _parse_window(parameters: Dict[str, Any]) -> int:
    window = parameters.get("window", DEFAULT_WINDOW)
    # bool is an int subclass; reject it and any non-integral number.
    if isinstance(window, bool) or not isinstance(window, (int, float)) \
            or not float(window).is_integer() or window < MIN_WINDOW:
        raise ValueError(
            "MotionRatePlugin parameter 'window' must be an integer "
            f">= {MIN_WINDOW} (default {DEFAULT_WINDOW}), got {window!r}"
        )
    return int(window)


def fit_line(times: Tuple[float, ...], values: Tuple[float, ...]) -> Tuple[float, float]:
    """
    Least-squares fit of ``value = slope * (time - times[0]) + offset``.

    Return ``(nan, nan)`` when fewer than two samples are given or all
    timestamps are equal, because the slope is undefined then.
    """
    if len(times) < MIN_WINDOW or len(times) != len(values):
        return math.nan, math.nan
    t = np.asarray(times, dtype=float)
    t -= t[0]  # keep float precision: POSIX seconds are ~1.7e9
    v = np.asarray(values, dtype=float)
    t_mean = t.mean()
    v_mean = v.mean()
    dt = t - t_mean
    sxx = float(np.dot(dt, dt))
    if not sxx > 0.0:
        return math.nan, math.nan
    slope = float(np.dot(dt, v - v_mean)) / sxx
    offset = float(v_mean - slope * t_mean)
    return slope, offset


@register_plugin("MotionRatePlugin")
class MotionRatePlugin(AsyncScanPlugin):
    """
    Fit the rate of change of ``signal`` against its detector timestamp.

    Columns: ``MotionRateStatus``, ``MotionRateIndex``, ``MotionRateSlope``
    (signal units per second) and ``MotionRateOffset`` (signal units at the
    first timestamp of the window).

    Scan thread (``build_data_snapshot``): read the value and its timestamp
    from the current row, append them to the window and copy the window.
    Worker thread (``process_data_snapshot``): fit the copied window only.
    """

    HEADERS: Tuple[str, ...] = (
        "MotionRateStatus",
        "MotionRateIndex",
        "MotionRateSlope",
        "MotionRateOffset",
    )

    def __init__(
        self,
        name: str,
        parameters: Optional[Dict[str, Any]] = None,
        scan: Optional[Any] = None,
    ) -> None:
        # Validate before super().__init__() so an invalid configuration does
        # not leave a started ThreadPoolExecutor behind.
        params = parameters or {}
        self._signal = _parse_signal(params)
        self._timestamp_key = TIMESTAMP_PREFIX + self._signal
        self._window = _parse_window(params)
        self._headers: List[str] = list(self.HEADERS)
        self._defaults = [STATUS_PENDING, -1, math.nan, math.nan]
        # Scan-thread state: only build_data_snapshot() and on_start() touch it.
        self._times: Deque[float] = deque(maxlen=self._window)
        self._values: Deque[float] = deque(maxlen=self._window)
        self._missing_reported = False
        super().__init__(name=name, parameters=params, scan=scan)

    @property
    def signal(self) -> str:
        return self._signal

    @property
    def window(self) -> int:
        return self._window

    def get_headers(self, timestamps: bool) -> List[str]:
        del timestamps  # plugin columns are value-only
        return self._headers

    def get_async_default_values(self) -> List[Any]:
        return list(self._defaults)

    def on_start(self) -> None:
        self._times.clear()
        self._values.clear()
        self._missing_reported = False

    def _read_sample(self) -> Optional[Tuple[float, float]]:
        """Return (timestamp, value) of the current point, or None if invalid."""
        scan = self.scan
        if scan is None:
            return None
        raw_value = scan.get_current_row_value(self._signal)
        raw_ts = scan.get_current_row_value(self._timestamp_key)
        try:
            value = float(raw_value)
            ts = float(raw_ts)
        except (TypeError, ValueError, OverflowError):
            return None
        if not (math.isfinite(value) and math.isfinite(ts)):
            return None
        return ts, value

    def build_data_snapshot(self, idx: int, pos: Any) -> Optional[Snapshot]:
        """Scan thread: add the current sample to the window and copy it."""
        del pos
        sample = self._read_sample()
        if sample is None:
            if not self._missing_reported:
                self._missing_reported = True
                self.logger.warning(
                    "No valid '%s'/'%s' in scan row at idx=%s; sample skipped "
                    "(reported once per scan)",
                    self._signal, self._timestamp_key, idx,
                )
            return None
        ts, value = sample
        # The same timestamp again means the PV has not updated since the
        # last point: the sample is a duplicate, not a new measurement, and
        # the window (and so the fit) is unchanged.
        if self._times and self._times[-1] == ts:
            return None
        self._times.append(ts)
        self._values.append(value)
        if len(self._times) < MIN_WINDOW:
            return None
        return idx, tuple(self._times), tuple(self._values)

    def process_data_snapshot(self, data_snapshot: Snapshot) -> List[Any]:
        """Worker thread: fit the copied window. Uses only the snapshot."""
        idx, times, values = data_snapshot
        slope, offset = fit_line(times, values)
        status = STATUS_READY if math.isfinite(slope) else STATUS_ERROR
        return [status, idx, slope, offset]
