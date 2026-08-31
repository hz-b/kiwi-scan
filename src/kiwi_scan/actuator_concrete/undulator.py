# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from typing import Sequence

from kiwi_scan.actuator.multi import MultiActuator
from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.epics_wrapper import EpicsPV

logger = logging.getLogger(__name__)

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

        jog_cfg = self.config.jog
        if jog_cfg is not None:
            self.jog_velocity_pv = (
                EpicsPV(jog_cfg.velocity_pv)
                if jog_cfg.velocity_pv
                else None
            )
            self.jog_command_pv = (
                EpicsPV(jog_cfg.command_pv)
                if jog_cfg.command_pv
                else None
            )
        else:
            self.jog_velocity_pv = None
            self.jog_command_pv = None

    @property
    def pvname(self) -> Sequence[str]:
        return [self._axis1.pvname, self._axis2.pvname]

    def run_move(self,
                 positions: Sequence[float],
                 sync: bool = True,
                 wait_startup: bool = False) -> None:
        if len(positions) != 2:
            raise ValueError("Undulator needs exactly 2 positions")
        gap, shift = positions
        raise NotImplementedError(f"run_move not fully implemented, target gap={gap}, shift={shift}")

    def _write_jog_velocities(self, velocities: Sequence[float]) -> bool:
        """Default: writes to jog_velocity_pv as a waveform (array of floats)."""
        arr = list(velocities)
        jog_velocity_pv = self.jog_velocity_pv
        if jog_velocity_pv is None:
            logger.error("No jog_velocity_pv configured")
            return False

        nelm = jog_velocity_pv.nelm
        if nelm is not None and len(arr) > nelm:
            raise ValueError(
                f"Waveform length {len(arr)} exceeds NELM ({nelm}) for PV {jog_velocity_pv.pvname}"
            )
        return jog_velocity_pv.put(arr)

    def _write_jog_command(self, velocities: Sequence[float]) -> bool:
        """Write the optional start command for jog operation."""
        jog_command_pv = self.jog_command_pv
        if jog_command_pv is None:
            logger.debug("No jog_command_pv, not writing jog start command.")
            return True 

        jog_cfg = self.config.jog
        if jog_cfg is None:
            logger.error("Jog command PV exists without jog configuration")
            return False

        velocity = velocities[0]
        if jog_cfg.command_pos is not None and jog_cfg.command_neg is not None:
            cmd = jog_cfg.command_pos if velocity >= 0 else jog_cfg.command_neg
        else:
            cmd = (
                jog_cfg.command_pos
                if jog_cfg.command_pos is not None
                else 1.0
            )

        success = jog_command_pv.put(cmd)
        if not success:
            logger.error("Failed to set jog start command for %s", jog_command_pv.pvname)
        return success

    def jog(self, velocities: Sequence[float], sync: bool = True) -> None:
        if len(velocities) != 2:
            raise ValueError("Undulator needs two velocities (gap, shift)")
        logger.info(f"Velocities: {velocities}")
        ok = self._write_jog_velocities(velocities)
        if not ok:
            raise RuntimeError(f"Failed to write jog velocities {velocities}.")

        self._write_jog_command(velocities)

        if sync:
            logger.debug("Jog sync=True: no sync implementation (override if needed)")

class UndulatorViaCAN(UndulatorViaEPICS):
    """
    CAN-bus variant: velocities are packed into a 32-bit int and written to the jog_command_pv.
    """
    
    @staticmethod
    def pack_velocities(vgap: float, vshift: float) -> int:
        """
        Pack gap/shift speed factors into CAN_BROADCAST_SPEED.
        Source: https://idcs-documentation.sourceforge.io/uniserv_can.html

        Each input is a multiplier in the range 0.0 .. 1.0:
          0.0 -> 0x0000
          1.0 -> 0xFFFF

        Bits 0..15 contain the gap multiplier.
        Bits 16..31 contain the shift multiplier.
        """

        def scaled_uint16(value: float) -> int:
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"CAN speed multiplier must be between 0.0 and 1.0, got {value}") 
            return round(value * 0xFFFF)

        gap = scaled_uint16(vgap)
        shift = scaled_uint16(vshift)
        packed = (shift << 16) | gap
        # EPICS long is signed 32 bit. Preserve the same 32 CAN bits.
        if packed >= 0x80000000:
            packed -= 0x100000000
        logger.debug("Packed CAN speed multipliers gap=%s shift=%s -> 0x%08X", vgap, vshift, packed & 0xFFFFFFFF)

        return packed

    def _write_jog_velocities(self, velocities: Sequence[float]) -> bool:
        if len(velocities) != 2:
            raise ValueError("UndulatorViaCAN needs two velocities (gap, shift)")
        packed = self.pack_velocities(velocities[0], velocities[1])
        if self.jog_velocity_pv is not None:
            return self.jog_velocity_pv.put(int(packed))
        else:
            logger.error("No jog_command_pv configured for this UndulatorViaCAN")
            return False

UNDULATOR_TYPES = {
    "epics": UndulatorViaEPICS,
    "can": UndulatorViaCAN,
}
