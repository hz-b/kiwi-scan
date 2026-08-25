# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import threading
import time
from typing import Any, Optional, Sequence

from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.datamodels import ActuatorConfig


class MultiActuator(AbstractActuator):
    """
    A composite actuator that lets you jog N child actuators in one call.
    """
    def __init__(self, actuators: Sequence[AbstractActuator], config: ActuatorConfig):
        super().__init__(config)
        self._axes = list(actuators)

    @property
    def pvname(self) -> Sequence[str]:
        return [ax.pvname for ax in self._axes]

    @property
    def rbv(self) -> Sequence[Any]:
        return [ax.rbv for ax in self._axes]

    @rbv.setter
    def rbv(self, values: Sequence[Any]) -> None:
        if len(values) != len(self._axes):
            raise ValueError(f"Expected {len(self._axes)} values, got {len(values)}")
        for ax, v in zip(self._axes, values):
            ax.rbv = v

    @property
    def cmdv(self) -> Sequence[Any]:
        return [ax.cmdv for ax in self._axes]

    @cmdv.setter
    def cmdv(self, values: Sequence[Any]) -> None:
        if len(values) != len(self._axes):
            raise ValueError(f"Expected {len(self._axes)} values, got {len(values)}")
        for ax, v in zip(self._axes, values):
            ax.cmdv = v

    # Implemented abstract methods:
    def set_velocity(self, velocities: Sequence[float]) -> None:
        if len(velocities) != len(self._axes):
            raise ValueError(f"Need {len(self._axes)} velocities, got {len(velocities)}")
        for ax, v in zip(self._axes, velocities):
            ax.set_velocity(v)

    def get_velocity(self) -> Sequence[Optional[float]]:
        return [ax.get_velocity() for ax in self._axes]

    def move(self, positions: Sequence[float]) -> None:
        # non-blocking multi-axis move
        self.run_move(positions, sync=False)

    def rel_move(self, deltas: Sequence[float]) -> None:
        """Non-blocking multi-axis relative move."""
        self.run_rel_move(deltas, sync=False)

    def run_move(
        self,
        positions: Sequence[float],
        sync: bool = True,
        wait_startup: bool = False,
    ) -> None:
        if len(positions) != len(self._axes):
            raise ValueError(f"Expected {len(self._axes)} positions, got {len(positions)}")
        # issue each child move without blocking
        for ax, pos in zip(self._axes, positions):
            ax.run_move(pos, sync=False)
        if sync:
            # wait until all axes are ready
            while not self.is_ready():
                time.sleep(0.01)
        elif wait_startup:
            self.wait_for_startup()

    def run_rel_move(
        self,
        deltas: Sequence[float],
        sync: bool = True,
        wait_startup: bool = False,
    ) -> None:
        if len(deltas) != len(self._axes):
            raise ValueError(f"Expected {len(self._axes)} deltas, got {len(deltas)}")
        # issue each child relative move without blocking
        for ax, d in zip(self._axes, deltas):
            ax.run_rel_move(float(d), sync=False)
        if sync:
            while not self.is_ready():
                time.sleep(0.01)
        elif wait_startup:
            self.wait_for_startup()

    def jog(self, velocities: Sequence[float], sync: bool = True) -> None:
        if len(velocities) != len(self._axes):
            raise ValueError(f"Need {len(self._axes)} velocities, got {len(velocities)}")
        for ax, v in zip(self._axes, velocities):
            ax.jog(v, sync=False)
        if sync:
            while not self.is_ready():
                time.sleep(0.01)

    def stop(self) -> None:
        for ax in self._axes:
            ax.stop()

    def is_ready(self) -> bool:
        # wait for all single actuator conditions
        return all(ax.is_ready() for ax in self._axes)

    def is_in_position(self, targets: Sequence[float], in_position_band: Optional[float] = None) -> bool:
        band = in_position_band if in_position_band is not None else self.in_position_band
        return all(ax.is_in_position(t, band) for ax, t in zip(self._axes, targets))

    def wait_for_startup(self, stop_event: Optional[threading.Event] = None) -> bool:
        """Wait until every child actuator reports observed startup."""
        ok = True
        for ax in self._axes:
            if stop_event is not None and stop_event.is_set():
                return False
            wait = getattr(ax, "wait_for_startup", None)
            if callable(wait):
                ok = bool(wait(stop_event=stop_event)) and ok
        return ok

    def wait_until_done(self, positions: Optional[Sequence[float]] = None) -> None:
        # wait until all axes have stopped, single actuators provide in position band etc.
        while not self.is_ready():
            time.sleep(0.01)

