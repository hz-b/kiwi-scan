# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""
An example plugin with simple closed-loop controller. This plugin is only used for tests and demonstartion.

Example Usage:
Mono test setup example config to make a device following mono energy:
scan_runner --config-file jogpid.yaml --scan_type cm --dim actuator=energy,start=300,stop=700,steps=0,velocity=0.01

# jogpid.yaml looks like
...
plugin_configs:
  - type: JogPIDPlugin
    name: jogpid
    parameters:
      actuator:
        ...
        jog:
          velocity_pv: "${IOC_MONO}:M1_SETJOGSPEED"
          abs_velocity: True
          command_pv: "${IOC_MONO}:M1_JOG_COMMAND"
          command_pos: 1
          command_neg: -1
...

This computes a new set-point with a PID + velocity feed-forward term and runs jog(). 
"""

import time
from typing import Any, Dict, List, Optional

from epics import PV

from kiwi_scan.actuator.factory import create_actuator
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import register_plugin
from kiwi_scan.scan.common import BaseScan


def _gain_source(gain_spec):
    """
    Support a numeric constant OR a PV name.
    """
    if gain_spec is None:
        return 0.0
    if isinstance(gain_spec, str):
        return PV(gain_spec)
    return float(gain_spec)


@register_plugin("JogPIDPlugin")
class JogPIDPlugin(ScanPlugin):
    """
    Generic PID controller executed at each scan point.
    """

    def __init__(
        self,
        name: str,
        parameters: Optional[Dict[str, Any]] = None,
        scan: Optional["BaseScan"] = None,
    ):
        super().__init__(name, parameters, scan)
        
        # ---------- Logging -------------------------------------------------
        self.DEFAULT_LOG_FILE = "JogPIDPlugin.log"
        self._init_logging()

        # ---------- Mandatory Actuator ---------------------------------------
        try:
            self.logger.info("%s", self.parameters["actuator"])
            self.actuator = create_actuator(ActuatorConfig.from_dict(self.parameters["actuator"]))
        except KeyError as missing:
            raise ValueError(f"JogPIDPlugin: missing parameter {missing!s}")

        # ---------- Gains ---------------------------------------------------
        self.kp  = _gain_source(self.parameters.get("kp", 0.001))
        self.ki  = _gain_source(self.parameters.get("ki", 0.0))
        self.kd  = _gain_source(self.parameters.get("kd", 0.0))
        self.kvf = _gain_source(self.parameters.get("kvf", 0.0))   # velocity FF

        # ---------- Internal state -----------------------------------------
        self.sample_time = float(self.parameters.get("sample_time", 1.0))
        if not self.sample_time > 0:
            raise ValueError("JogPIDPlugin: sample_time must be > 0")
        self.integral    = 0.0
        self.prev_error  = 0.0
        self.prev_time   = None
        self.prev_set_time = None
        self.setpoint = float("nan")

        self.logger.debug("JogPIDPlugin initialised with parameters: %s", self.parameters)

    # ------------------------------------------------------------------ API
    def get_headers(self, timestamps: bool) -> List[str]:
        return ["ControllerSetpoint"] + (["TS_ControllerSetpoint"] if timestamps else [])

    def get_values(self, idx: int, pos: Dict[str, Any]) -> List[Any]:
        """
        Called once per scan point. Compute and write a new setpoint when the
        control interval is due; otherwise return the most recent setpoint.
        """
        now = time.time()
        if (self.prev_set_time is not None and now - self.prev_set_time < self.sample_time):
            return [self.setpoint]

        try:
            position  = self.actuator.rbv 
            velocity = self.actuator.get_velocity() or 0.0
            target    = float(pos)    # Exapmple set position to follow first actuator in this case
        except Exception as e: # noqa BLE001
            self.logger.error("PV read failed @ point %s: %s", idx, e)
            return [float("nan")]

        self.logger.info("pos: %s, target: %s", pos, target)
        self.setpoint = self._calculate_setpoint(position, velocity, target, now)
        self.actuator.jog(self.setpoint, sync=False)
        self.prev_set_time = now

        self.logger.debug( "[%d] pos=%.6g vel=%.6g tgt=%.6g sp=%.6g err=%.6g", idx, position, velocity, target, self.setpoint, target - position,)
        return [self.setpoint]

    def _calculate_setpoint(
        self,
        position: float,
        velocity: float,
        target: float,
        now: float,
    ) -> float:
        """Calculate one PID control update and advance the controller state."""
        # Convert gain PVs to numeric if necessary --------------------------
        def g(val):
            return val.get() if hasattr(val, "get") else val

        kp, ki, kd, kvf = map(g, (self.kp, self.ki, self.kd, self.kvf))

        # Basic PID + velocity FF ------------------------------------------
        error = target - position
        dt    = (now - self.prev_time) if self.prev_time else self.sample_time

        self.integral += error * dt
        derivative     = (error - self.prev_error) / dt if dt > 0 else 0.0

        setpoint = (
            kp  * error +
            ki  * self.integral +
            kd  * derivative +
            kvf * velocity          # feed-forward term
        )

        # State update ------------------------------------------------------
        self.prev_error = error
        self.prev_time  = now
        return setpoint

    # ------------------------------------------------------------------ Hooks
    def on_start(self) -> None:
        self.logger.info("JogPIDPlugin started")

    def on_end(self) -> None:
        try:
            self.actuator.stop()
        finally:
            self.logger.info("JogPIDPlugin finished")
