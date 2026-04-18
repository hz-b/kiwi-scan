# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Callable, Dict, Mapping
import threading
import time
import logging

from kiwi_scan.datamodels import ActuatorConfig


@dataclass(frozen=True)
class PvEvent:
    """
    Representation of one PV update.
    This decouples from the raw pyepics callback kwargs.
    """
    pvname: str
    value: Any
    source: Optional[str] = None

    # Time
    timestamp: Optional[float] = None   # seconds since epoch (float)
    posixseconds: Optional[int] = None
    nanoseconds: Optional[int] = None

    # Alarm / status
    severity: Optional[int] = None
    status: Optional[int] = None
    
    # Anything else provided by the backend
    raw: Mapping[str, Any] = None


MonitorCallback = Callable[[PvEvent], None]


class AbstractActuator(ABC):
    """
    Defines the interface for an actuator.
    """

    def __init__(self, config: ActuatorConfig):
        self.config = config
        # Expose common config parameters
        self.backlash: float = config.backlash
        self.in_position_band: float = config.in_position_band
        self.dwell_time: float = config.dwell_time
        self.ready_value: Any = config.ready_value
        self.startup_timeout: float = config.startup_timeout
        self.q_delay: float = config.queueing_delay

        # --- Monitoring  -----------------------
        self._monitors: Dict[str, Any] = {}                 # pvname -> backend monitor object (e.g. EpicsPV)
        self._monitor_callbacks: Dict[str, list[MonitorCallback]] = {}
        self._last_events: Dict[str, PvEvent] = {}
        self._monitor_lock = threading.RLock()

    # --------------------- monitor/event helpers ----------------------------

    def supports_monitors(self) -> bool:
        """Override in backends that can subscribe to PV updates (e.g. EPICS)."""
        return False

    def add_monitor(self, pvname: str, user_callback: Optional[MonitorCallback] = None, **kwargs) -> Any:
        """
        Subscribe to PV updates. Concrete backends (EpicsActuator) should override.
        Returns a backend-specific monitor handle/object (e.g. EpicsPV).
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support PV monitors")

    def remove_monitor(self, pvname: str) -> None:
        """
        Remove/close a monitor subscription for pvname if supported by backend.
        Concrete backends should override if they can actually clear CA subscriptions.
        """
        with self._monitor_lock:
            self._monitors.pop(pvname, None)
            self._monitor_callbacks.pop(pvname, None)
            self._last_events.pop(pvname, None)

    def clear_monitors(self) -> None:
        """Remove all monitors."""
        with self._monitor_lock:
            pvs = list(self._monitors.keys())
        for pv in pvs:
            self.remove_monitor(pv)

    def on_pv_event(self, pvname: str, cb: MonitorCallback) -> None:
        """Register an additional listener for pvname (even if monitor already exists)."""
        with self._monitor_lock:
            self._monitor_callbacks.setdefault(pvname, []).append(cb)

    def get_last_event(self, pvname: str) -> Optional[PvEvent]:
        with self._monitor_lock:
            return self._last_events.get(pvname)

    def _dispatch_pv_update(self, pvname: str, value: Any, **kw: Any) -> PvEvent:
        """
        Backend-facing entry point: call this from EPICS callbacks.
        Stores last event and notifies listeners.
        """
        ev = PvEvent(
            pvname=pvname,
            value=value,
            timestamp=kw.get("timestamp", None),
            posixseconds=kw.get("posixseconds", None),
            nanoseconds=kw.get("nanoseconds", None),
            severity=kw.get("severity", None),
            status=kw.get("status", None),
            source=kw.get("source", None),
            raw=dict(kw) if kw else {},
        )

        with self._monitor_lock:
            self._last_events[pvname] = ev
            listeners = list(self._monitor_callbacks.get(pvname, []))

        # Call listeners outside the lock
        for cb in listeners:
            try:
                cb(ev)
            except Exception:
                # Keep it non-fatal: monitor callbacks must never kill control logic
                logging.exception(f"Monitor callback failed for {pvname}")

        return ev
    @property
    @abstractmethod
    def pvname(self) -> str:
        """The primary PV name for the actuator."""
        pass

    @property
    @abstractmethod
    def rbv(self) -> Optional[Any]:
        """Read-back value shortcut property."""
        pass

    @rbv.setter
    @abstractmethod
    def rbv(self, value: Any) -> None:
        """Read-back value shortcut property setter."""
        pass

    @property
    @abstractmethod
    def cmdv(self) -> Optional[Any]:
        """Commanded value shortcut property."""
        pass

    @cmdv.setter
    @abstractmethod
    def cmdv(self, value: Any) -> None:
        """Commanded value shortcut property setter."""
        pass

    @abstractmethod
    def set_velocity(self, velocity: float) -> None:
        """Set the actuator velocity (for move and jog)."""
        pass

    @abstractmethod
    def get_velocity(self) -> Optional[float]:
        """Get the current actuator velocity."""
        pass

    @abstractmethod
    def move(self, position: float) -> None:
        """Issue a move command without waiting."""
        pass

    @abstractmethod
    def rel_move(self, delta: float) -> None:
        """Issue a *relative* move command without waiting (incremental move)."""
        pass

    @abstractmethod
    def run_move(self, position: float, sync: bool = True) -> None:
        """Move the actuator, optionally waiting until completion."""
        pass

    @abstractmethod
    def run_rel_move(self, delta: float, sync: bool = True) -> None:
        """Relative move, optionally waiting until completion."""
        pass

    @abstractmethod
    def jog(self, velocity: float, sync: bool = True) -> None:
        """Continuous jog at given velocity."""
        pass

    @abstractmethod
    def is_ready(self) -> bool:
        """Return True if actuator is ready (not is_movingy())."""
        pass
    
    def is_moving(self) -> bool:
        return not self.is_ready()

    @abstractmethod
    def is_in_position(self, target: float, in_position_band: float) -> bool:
        """Check if readback is within band of target."""
        pass

    @abstractmethod
    def wait_until_done(self, position: float) -> None:
        """Block until actuator has reached target."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop motion immediately."""
        pass
