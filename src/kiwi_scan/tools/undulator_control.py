# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT


import logging
import math
from dataclasses import replace
from typing import Dict, Mapping, Optional, Sequence, Tuple

from .axis_pid import (
    AxisPidGains,
    AxisPidInputs,
    AxisPidResult,
    AxisPidState,
    compute_axis,
)

logger = logging.getLogger(__name__)


class UndulatorControlCore:
    """Hardware-independent PID control for gap-only or gap/shift devices.
    One controller thread must supply a timestamp in seconds to calculate()
    """

    def __init__(
        self,
        enabled_axes: Sequence[str] = ("gap", "shift"),
        initial_dt: float = 1.0,
    ) -> None:
        if isinstance(enabled_axes, str):
            raise TypeError("enabled_axes must be a sequence, e.g. ['gap']")
        axes = tuple(enabled_axes)
        if axes not in (("gap",), ("gap", "shift"), ("shift", "gap")):
            raise ValueError("enabled_axes must contain gap and optionally shift, without duplicates")
        if not math.isfinite(initial_dt) or initial_dt <= 0:
            raise ValueError("initial_dt must be finite and positive")
        self._enabled_axes = tuple(axis for axis in ("gap", "shift") if axis in axes)
        self._initial_dt = initial_dt
        self._previous_calculation_time: Optional[float] = None
        self._states: Dict[str, AxisPidState] = {}
        self.reset()

    @property
    def enabled_axes(self) -> Tuple[str, ...]:
        return self._enabled_axes

    @property
    def states(self) -> Mapping[str, AxisPidState]:
        return dict(self._states)

    def reset(self) -> None:
        """ Reset controller, keep gains. """
        self._states = {axis: AxisPidState() for axis in self._enabled_axes}
        self._previous_calculation_time = None
        logger.debug("Undulator controller reset: enabled_axes=%s initial_dt=%.6g; history cleared",
            self._enabled_axes, self._initial_dt)

    def sample_interval(self, now: float) -> float:
        """ Match the existing first-cycle and minimum-dt rules."""
        if not math.isfinite(now):
            logger.debug("Undulator controller rejected non-finite timestamp: now=%r", now)
            raise ValueError("calculation timestamp must be finite")
        if self._previous_calculation_time is None:
            return self._initial_dt
        return max(now - self._previous_calculation_time, 1e-6)

    def calculate(
        self,
        inputs: Mapping[str, AxisPidInputs],
        gains: Mapping[str, AxisPidGains],
        now: float,
    ) -> Dict[str, AxisPidResult]:
        """ Calculate gap and shift if enabled (self._enabled_axes) """
        dt = self.sample_interval(now)
        for axis in self._enabled_axes:
            if axis not in inputs or axis not in gains:
                logger.debug("Undulator cycle rejected: now=%.9f axis=%s has_inputs=%s has_gains=%s; history unchanged",
                    now, axis, axis in inputs, axis in gains)
                raise ValueError("Missing inputs or gains for enabled axis: " + axis)
        results = {}
        next_states = {}
        for axis in self._enabled_axes:
            try:
                results[axis], next_states[axis] = compute_axis(
                    inputs=replace(inputs[axis], dt=dt),
                    gains=gains[axis],
                    state=self._states[axis],
                )
            except Exception as exc:
                logger.debug("Undulator calculation failed: now=%.9f axis=%s dt=%.6g error=%s; all axis histories unchanged",
                    now, axis, dt, exc, exc_info=True)
                raise
        # previous_time = self._previous_calculation_time
        self._states = next_states
        self._previous_calculation_time = now
        ## HOT PATH
        #if logger.isEnabledFor(logging.DEBUG):
        #    timing = (
        #        "initial" if previous_time is None
        #        else "clamped" if now - previous_time < 1e-6
        #        else "elapsed"
        #    )
        #    logger.debug("Undulator cycle committed: now=%.9f previous=%s dt=%.6g timing=%s calculated_commands=%s",
        #        now, previous_time, dt, timing, {axis: result.command for axis, result in results.items()})
        return results
