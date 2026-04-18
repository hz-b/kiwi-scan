# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""
jog_pid.py
A generic example closed-loop controller for ScanLib.

Reads:
    • Actuator
    • (Optionally) gain PVs for Kp, Ki, Kd, Kvf

Computes a new set-point with a PID + velocity feed-forward term and
runs jog(). The control law is executed once for every sample period at a
scan point via ScanPlugin.get_values().
"""

import time
import logging
import os
from typing import Dict, Any, Optional, List

from epics import PV

from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import register_plugin, PluginConfig 
from kiwi_scan.epics_wrapper import EpicsPV
from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.actuator.factory import create_actuator
from kiwi_scan.datamodels import ActuatorConfig

def _gain_source(gain_spec):
    """
    Helper: support a numeric constant OR a PV name.
    Returns either a float (constant) or an epics.PV instance.
    TODO: share move to base
    """
    if gain_spec is None:
        return 0.0
    if isinstance(gain_spec, str):
        return PV(gain_spec)
    return float(gain_spec)


@register_plugin("JogPIDPlugin")
class JogPIDPlugin(ScanPlugin):
    """
    Generic PID + velocity-feed-forward controller executed at each scan point. TODO: optionally at monitor event.
    """

    def __init__(
        self,
        name: str,
        parameters: Optional[Dict[str, Any]] = None,
        scan: Optional["BaseScan"] = None,
    ):
        super().__init__(name, parameters, scan)
        

        scan_config = self.scan.cfg
        # ---------- Logging -------------------------------------------------
        log_level = (
            self.parameters.get("log_level")
            or getattr(scan_config, "logging_level", logging.INFO)
        )
        log_file = os.path.join(
            self.log_dir, self.parameters.get("log_file", "jogpid_plugin.txt")
        )
        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            hdlr = logging.FileHandler(log_file)
            hdlr.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                )
            )
            self.logger.addHandler(hdlr)

        # ---------- Mandatory Actuator ---------------------------------------
        try:
            logging.info(f"{self.parameters['actuator']}")
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
        self.integral    = 0.0
        self.prev_error  = 0.0
        self.prev_time   = None
        self.prev_set_time = None

        self.logger.debug(
            "JogPIDPlugin initialised with parameters: %s", self.parameters
        )

    # ------------------------------------------------------------------ API
    def get_headers(self, timestamps: bool) -> List[str]:
        return ["ControllerSetpoint"] + (["TS_ControllerSetpoint"] if timestamps else [])

    def get_values(self, idx: int, pos: Dict[str, Any]) -> List[Any]:
        """
        Called once per scan point.  Computes new set-point, writes it, and
        returns the value for recording.
        """
        now = time.time()

        try:
            position  = self.actuator.rbv 
            velocity = self.actuator.get_velocity() or 0.0
            target    = float(pos)
        except Exception as e:
            self.logger.error("PV read failed @ point %s: %s", idx, e)
            return [float("nan")]

        logging.info(f"pos: {pos}, target: {target}")
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

        # Write new set-point ----------------------------------------------
        if not self.prev_set_time or (now - self.prev_set_time) > self.sample_time:
            self.actuator.jog(setpoint, sync=False)
            self.prev_set_time = now

        # State update ------------------------------------------------------
        self.prev_error = error
        self.prev_time  = now

        # Log & return ------------------------------------------------------
        self.logger.debug(
            "[%d] pos=%.6g vel=%.6g tgt=%.6g sp=%.6g err=%.6g",
            idx, position, velocity, target, setpoint, error
        )
        return [setpoint]

    # ------------------------------------------------------------------ Hooks
    def on_start(self) -> None:
        self.logger.info("JogPIDPlugin started")

    def on_end(self) -> None:
        self.logger.info("JogPIDPlugin finished")

