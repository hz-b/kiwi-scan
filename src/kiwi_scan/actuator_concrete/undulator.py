# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from typing import Sequence, Optional, Any

import numpy as np
from kiwi_scan.actuator.multi import MultiActuator
from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.epics_wrapper import EpicsPV

class UndulatorViaEPICS(MultiActuator):
    """
    Gap/Shift of undulator (2-axis).
    """

    def __init__(self,
                 axis1: AbstractActuator,
                 axis2: AbstractActuator,
                 config: ActuatorConfig):
        super().__init__([axis1, axis2], config)
        self._axis1 = axis1
        self._axis2 = axis2

        jog_cfg = getattr(self.config, 'jog', None)
        if jog_cfg:
            self.jog_velocity_pv = EpicsPV(jog_cfg.velocity_pv) if getattr(jog_cfg, 'velocity_pv', None) else None
            self.jog_command_pv = EpicsPV(jog_cfg.command_pv) if getattr(jog_cfg, 'command_pv', None) else None
        else:
            self.jog_velocity_pv = None
            self.jog_command_pv = None

    @property
    def pvname(self) -> Sequence[str]:
        return [self._axis1.pvname, self._axis2.pvname]

    def run_move(self,
                 positions: Sequence[float],
                 sync: bool = True) -> None:
        if len(positions) != 2:
            raise ValueError("Undulator needs exactly 2 positions")
        gap, shift = positions
        raise NotImplementedError(f"run_move not fully implemented, target gap={gap}, shift={shift}")

    def _write_jog_velocities(self, velocities: Sequence[float]) -> bool:
        """Default: writes to jog_velocity_pv as a waveform (array of floats)."""
        arr = list(velocities)
        if self.jog_velocity_pv is not None:
            nelm = getattr(self.jog_velocity_pv, 'nelm', None)
            if nelm is not None and len(arr) > nelm:
                raise ValueError(
                    f"Waveform length {len(arr)} exceeds NELM ({nelm}) for PV {self.jog_velocity_pv.pvname}"
                )
            return self.jog_velocity_pv.put(arr)
        else:
            logging.error("No jog_velocity_pv configured")
            return False

    def _write_jog_command(self, velocities: Sequence[float]) -> bool:
        """Writes the start command for jog operation (if required)."""
        jog_cfg = getattr(self.config, 'jog', None)
        if not self.jog_command_pv:
            logging.debug("No jog_command_pv, not writing jog start command.")
            return True  # Not an error; just nothing to do.

        velocity = velocities[0]
        if hasattr(jog_cfg, 'command_pos') and hasattr(jog_cfg, 'command_neg'):
            if jog_cfg.command_pos is not None and jog_cfg.command_neg is not None:
                cmd = jog_cfg.command_pos if velocity >= 0 else jog_cfg.command_neg
            else:
                cmd = getattr(jog_cfg, 'command_pos', 1.0) or 1.0
        else:
            cmd = getattr(jog_cfg, 'command_pos', 1.0) or 1.0
        return self.jog_command_pv.put(cmd)

    def jog(self, velocities: Sequence[float], sync: bool = True) -> None:
        if len(velocities) != 2:
            raise ValueError("Undulator needs two velocities (gap, shift)")
        logging.info(f"Velocities: {velocities}")
        ok = self._write_jog_velocities(velocities)
        if not ok:
            raise RuntimeError(f"Failed to write jog velocities {velocities}.")

        success = self._write_jog_command(velocities)
        if not success:
            logging.error(f"Failed to set jog start command for {self.jog_command_pv.pvname}")

        if sync:
            logging.debug("Jog sync=True: no sync implementation (override if needed)")

class UndulatorViaCAN(UndulatorViaEPICS):
    """
    CAN-bus variant: velocities are packed into a 32-bit int and written to the jog_command_pv.
    """
    
    @staticmethod
    def pack_velocities(vgap: float, vshift: float) -> int:
        def to_int16(val):
            val = int(round(val))
            return max(-32768, min(32767, val))
        vgap_int16 = to_int16(vgap)
        vshift_int16 = to_int16(vshift)
        return ((vshift_int16 & 0xFFFF) << 16) | (vgap_int16 & 0xFFFF)

    def _write_jog_velocities(self, velocities: Sequence[float]) -> bool:
        if len(velocities) != 2:
            raise ValueError("UndulatorViaCAN needs two velocities (gap, shift)")
        packed = self.pack_velocities(velocities[0], velocities[1])
        if self.jog_command_pv is not None:
            return self.jog_command_pv.put(int(packed))
        else:
            logging.error("No jog_command_pv configured for this UndulatorViaCAN")
            return False

UNDULATOR_TYPES = {
    "epics": UndulatorViaEPICS,
    "can": UndulatorViaCAN,
}
