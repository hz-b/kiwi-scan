# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from typing import Optional, Sequence

from kiwi_scan.actuator.multi import MultiActuator
from kiwi_scan.actuator.single import AbstractActuator, _close_created_pvs
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.epics_wrapper import EpicsPV

logger = logging.getLogger(__name__)

class UndulatorViaEPICS(MultiActuator):
    """
    Gap/Shift undulator; axis2 may be None for a gap-only device.
    The jog protocol still uses two slots, with zero in the disabled shift slot.
    """

    def __init__(self,
                 axis1: AbstractActuator,
                 axis2: Optional[AbstractActuator],
                 config: ActuatorConfig):
        super().__init__([axis1] if axis2 is None else [axis1, axis2], config)
        self._axis1 = axis1
        self._axis2 = axis2

        created_pvs = []

        def create_pv(pvname: str, **kwargs):
            pv = EpicsPV(pvname, **kwargs)
            created_pvs.append(pv)
            return pv

        try:
            jog_cfg = self.config.jog
            if jog_cfg is not None:
                self.jog_velocity_pv = (
                    create_pv(jog_cfg.velocity_pv, queueing_delay=self.q_delay)
                    if jog_cfg.velocity_pv
                    else None
                )
                self.jog_command_pv = (
                    create_pv(jog_cfg.command_pv, queueing_delay=self.q_delay)
                    if jog_cfg.command_pv
                    else None
                )
            else:
                self.jog_velocity_pv = None
                self.jog_command_pv = None
        except BaseException:
            _close_created_pvs(created_pvs)
            raise

    def set_queueing_delay(self, delay: float) -> None:
        """Set child delays and the shared EPICS/CAN output-PV delays."""
        super().set_queueing_delay(delay)
        for pv in (self.jog_velocity_pv, self.jog_command_pv):
            if pv is not None:
                pv.queueing_delay = self.q_delay

    @property
    def pvname(self) -> Sequence[str]:
        return [axis.pvname for axis in self._axes]

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
        if self._axis2 is None and velocities[1] != 0.0:
            raise ValueError("Gap-only undulator requires zero shift velocity")
        logger.debug("Jog speed multipliers: %s", velocities)
        ok = self._write_jog_velocities(velocities)
        if not ok:
            raise RuntimeError(f"Failed to write jog velocities {velocities}.")

        if not self._write_jog_command(velocities):
            raise RuntimeError("Failed to write undulator jog start command")

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

class UndulatorViaAxes(MultiActuator):
    """
    Write CAN-scaled speed multipliers through individual axis jog PVs.
    Outputs one or two normalized multipliers (gap, shift) in the range 0..65535
    """

    def __init__(
        self,
        axis1: AbstractActuator,
        axis2: Optional[AbstractActuator],
        config: ActuatorConfig,
    ):
        super().__init__([axis1] if axis2 is None else [axis1, axis2], config)
        self._axis1 = axis1
        self._axis2 = axis2
        self._axis_names = ("gap",) if axis2 is None else ("gap", "shift")
        for name, axis in zip(self._axis_names, self._axes):
            jog_cfg = axis.config.jog
            if jog_cfg is None or not jog_cfg.velocity_pv:
                raise ValueError(name + ".jog.velocity_pv is required for subtype axes")
            logger.debug(
                "Undulator axis %s: jog velocity PV=%s, command PV=%s, scale=%d",
                name, jog_cfg.velocity_pv, jog_cfg.command_pv, 0xFFFF,
            )

    def jog(self, velocities: Sequence[float], sync: bool = True) -> None:
        """ Write all enabled axes """
        if len(velocities) != 2:
            raise ValueError("Undulator needs two velocities (gap, shift)")
        if self._axis2 is None and velocities[1] != 0.0:
            raise ValueError("Gap-only undulator requires zero shift velocity")

        # Validate BOTH, invalid shift must not leave only the gap updated.
        encoded = []
        for value in velocities:
            if not 0.0 <= value <= 1.0:
                raise ValueError("Speed multiplier must be between 0.0 and 1.0, got " + str(value))
            encoded.append(round(value * 0xFFFF))

        logger.debug("Per-axis jog multipliers %s -> %s", velocities, encoded)
        for name, axis, value in zip(self._axis_names, self._axes, encoded):
            if axis.jog(value, sync=False) is False:
                raise RuntimeError("Failed to write " + name + " jog output")
        if sync:
            logger.debug("Jog sync=True: per-axis output writes do not wait for motion")


UNDULATOR_TYPES = {
    "epics": UndulatorViaEPICS,
    "can": UndulatorViaCAN,
    "axes": UndulatorViaAxes,
}
