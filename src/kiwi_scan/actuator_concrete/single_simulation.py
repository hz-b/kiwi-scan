# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import time
import logging
import threading
from typing import Optional, Any

from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.actuator.single import AbstractActuator


class SimulatedActuator(AbstractActuator):
    """
    A simple simulation of an actuator
    """

    def __init__(self, config: ActuatorConfig):
        super().__init__(config)

        # Internal state
        self._rbv: float = getattr(config, 'initial_position', 0.0)
        self._cmdv: float = self._rbv
        self._velocity: float = config.velocity or 1000.0
        self._moving: bool = False

    def supports_monitors(self) -> bool:
        return False
    
    @property
    def pvname(self) -> str:
        # Simulated PV name indicator
        return f"SIM:{self.config.pv}"

    @property
    def rbv(self) -> Optional[Any]:
        return self._rbv

    @rbv.setter
    def rbv(self, value: Any) -> None:
        self._rbv = float(value)

    @property
    def cmdv(self) -> Optional[Any]:
        return self._cmdv

    @cmdv.setter
    def cmdv(self, value: Any) -> None:
        self._cmdv = float(value)

    def set_velocity(self, velocity: float) -> None:
        self._velocity = float(velocity)
        logging.info(f"[SIM] Velocity set to {self._velocity}")

    def get_velocity(self) -> Optional[float]:
        return self._velocity

    def move(self, position: float) -> None:
        """Issue a move: set command value, mark as moving."""
        
        logging.info(f"[SIM] Commanded move to {position}")
        self.cmdv = position
        self._moving = True

    def rel_move(self, delta: float) -> None:
        target = float(self.rbv or 0.0) + float(delta)
        self.move(target)

    def run_move(
        self,
        position: float,
        sync: bool = True,
        wait_startup: bool = False,
    ) -> None:
        """Move and optionally wait for startup or completion."""
        self.move(position)
        if sync:
            self.wait_until_done(position)
        elif wait_startup:
            self.wait_for_startup()

    def run_rel_move(
        self,
        delta: float,
        sync: bool = True,
        wait_startup: bool = False,
    ) -> None:
        target = float(self.rbv or 0.0) + float(delta)
        self.move(target)
        if sync:
            self.wait_until_done(target)
        elif wait_startup:
            self.wait_for_startup()

    def jog(self, velocity: float, sync: bool = True) -> None:
        """Simulate a jog by a single step equal to velocity."""
        logging.info(f"[SIM] Jog with velocity {velocity}")
        if velocity == 0:
            self.stop()
            return
        # Treat jog as a one-step move
        target = self._rbv + velocity
        self.move(target)
        if sync:
            self.wait_until_done(target)

    def is_ready(self) -> bool:
        """ 
        Motor status matches ready_value. ``ready_value: -1`` keeps the actuator permanently
        not-ready for simulated endless motion.
        """
        status_value = int(self._moving)

        try:
            return float(status_value) == float(self.ready_value)
        except (TypeError, ValueError):
            return str(status_value).strip() == str(self.ready_value).strip()

    def is_in_position(self, target: float, in_position_band: float) -> bool:
        return abs(self._rbv - target) <= in_position_band

    def wait_for_startup(self, stop_event: Optional[threading.Event] = None) -> bool:
        """Wait until simulated motion has started."""
        start = time.monotonic()
        while not self._moving:
            if stop_event is not None and stop_event.is_set():
                return False
            if time.monotonic() - start > self.startup_timeout:
                return False
            time.sleep(0.01)
        return True

    def wait_until_done(self, position: float) -> None:
        """Wait until the simulated move completes."""
        if not self._moving:
            return
        # Simulate motion duration based on velocity
        distance = abs(position - self._rbv)
        # TODO: update _rbv
        sleep_time = distance / self._velocity if self._velocity > 0 else 0
        logging.info(f"[SIM] Moving for {sleep_time:.3f}s")
        time.sleep(sleep_time)
        # Arrive at position
        self._rbv = position
        self._moving = False
        # Dwell if configured
        if self.config.dwell_time > 0:
            logging.info(f"[SIM] Dwell for {self.config.dwell_time}s")
            time.sleep(self.config.dwell_time)
        logging.info(f"[SIM] Reached position {self._rbv}")

    def stop(self) -> None:
        """Stop motion immediately."""
        if self._moving:
            self._moving = False
            logging.info("[SIM] Motion stopped")
        else:
            logging.debug("[SIM] Stop called, but actuator was not moving")

