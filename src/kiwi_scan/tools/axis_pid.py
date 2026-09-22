# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AxisPidInputs:
    """
    Values sampled for one controller cycle.
    """

    command: float
    command_gradient: float
    readback: float
    target_current: float
    current: float
    maximum_velocity: float
    dt: float


@dataclass(frozen=True)
class AxisPidGains:
    """
    Controller gains and signal-shaping parameters.
    """

    kp: float = 0.0
    ki: float = 0.0
    kd: float = 0.0
    kvf: float = 0.0
    kvff: float = 0.0
    analog_offset: float = 0.0
    integral_limit: float = 0.0
    deadband: float = 0.0
    filter_alpha: float = 0.0
    maximum_delta: float = 0.0


@dataclass(frozen=True)
class AxisPidState:
    """
    Persistent history passed between controller cycles.
    """

    integral: float = 0.0
    previous_error: float = 0.0
    previous_command: Optional[float] = None
    previous_readback: Optional[float] = None
    filtered_command: Optional[float] = None
    rate_limited_command: Optional[float] = None


@dataclass(frozen=True)
class AxisPidResult:
    """
    Calculated command and diagnostic terms for one controller cycle.
    """

    command: float
    following_error: float
    proportional_term: float
    integral_term: float
    derivative_term: float
    velocity_term: float
    velocity_feedforward_term: float
    command_rate: float
    readback_rate: float
    raw_command: float
    direction_sign: int


def _signum(value: float) -> int:
    return (value > 0.0) - (value < 0.0)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))

# PID terms are needed for readability and diagnostics:
# pylint: disable-next=too-many-locals
def compute_axis(
    inputs: AxisPidInputs,
    gains: AxisPidGains,
    state: AxisPidState,
) -> Tuple[AxisPidResult, AxisPidState]:
    """
    This is made for a controller plugin for undulator control specific to HZB undulators.
    Signals are clamped to the input logic of the undulator CAN interface.
    Calculate one axis: The returned state must be supplied to the next call for the same axis.
    """
    if inputs.dt <= 0.0:
        raise ValueError("PID sample interval dt must be greater than zero")

    raw_error = inputs.command - inputs.readback
    following_error = (
        0.0
        if gains.deadband > 0.0 and abs(raw_error) < gains.deadband
        else raw_error
    )

    integral = state.integral + following_error * inputs.dt
    if gains.integral_limit > 0.0:
        integral = _clamp(integral, -gains.integral_limit, gains.integral_limit)

    derivative = (following_error - state.previous_error) / inputs.dt

    previous_command = ( inputs.command
        if state.previous_command is None
        else state.previous_command
    )
    previous_readback = (
        inputs.readback
        if state.previous_readback is None
        else state.previous_readback
    )
    command_rate = (inputs.command - previous_command) / inputs.dt
    readback_rate = (inputs.readback - previous_readback) / inputs.dt

    proportional_term = gains.kp * following_error
    integral_term = gains.ki * integral
    derivative_term = gains.kd * derivative
    velocity_term = gains.kvf * inputs.maximum_velocity
    velocity_feedforward_term = gains.kvff * inputs.command_gradient
    direction_sign = _signum(inputs.target_current - inputs.current)

    raw_command = (proportional_term + integral_term + derivative_term + velocity_term + velocity_feedforward_term - gains.analog_offset) * direction_sign

    saturated_command = _clamp(raw_command, 0.001, 1.0)
    if gains.filter_alpha <= 0.0 or state.filtered_command is None:
        filtered_command = saturated_command
    else:
        filtered_command = (gains.filter_alpha * state.filtered_command + (1.0 - gains.filter_alpha) * saturated_command)

    if ( gains.maximum_delta <= 0.0 or state.rate_limited_command is None):
        command = filtered_command
    else:
        delta = _clamp(filtered_command - state.rate_limited_command, -gains.maximum_delta, gains.maximum_delta)
        command = state.rate_limited_command + delta

    result = AxisPidResult(
        command=command,
        following_error=following_error,
        proportional_term=proportional_term,
        integral_term=integral_term,
        derivative_term=derivative_term,
        velocity_term=velocity_term,
        velocity_feedforward_term=velocity_feedforward_term,
        command_rate=command_rate,
        readback_rate=readback_rate,
        raw_command=raw_command,
        direction_sign=direction_sign,
    )
    next_state = AxisPidState(
        integral=integral,
        previous_error=following_error,
        previous_command=inputs.command,
        previous_readback=inputs.readback,
        filtered_command=filtered_command,
        rate_limited_command=command,
    )

    # HOT PATH
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Axis PID dt=%.6g cmd=%.6g rb=%.6g err=%.6g integral=%.6g "
            "terms[p=%.6g i=%.6g d=%.6g vf=%.6g vff=%.6g] "
            "raw=%.6g filtered=%.6g out=%.6g sign=%d",
            inputs.dt,
            inputs.command,
            inputs.readback,
            following_error,
            integral,
            proportional_term,
            integral_term,
            derivative_term,
            velocity_term,
            velocity_feedforward_term,
            raw_command,
            filtered_command,
            command,
            direction_sign,
        )

    return result, next_state


__all__ = [
    "AxisPidGains",
    "AxisPidInputs",
    "AxisPidResult",
    "AxisPidState",
    "compute_axis",
]
