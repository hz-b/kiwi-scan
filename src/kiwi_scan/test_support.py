from __future__ import annotations

from types import ModuleType, SimpleNamespace
from typing import Any, Callable, Dict, List, Optional


class FakeRawPV:
    """Minimal raw-PV stub used behind fake EpicsPV-like wrappers."""

    def __init__(self) -> None:
        self.removed: List[int] = []
        self.cleared = False
        self.disconnected = False

    def remove_callback(self, idx: int) -> None:
        self.removed.append(idx)

    def clear_callbacks(self) -> None:
        self.cleared = True

    def disconnect(self) -> None:
        self.disconnected = True


class FakePV:
    """
    Emulate the behavior and create role-specific subclasses needed by the current tests:
      * SubscriptionManager: EpicsPV
      * EpicsActuator: monitor PV wrappers
      * TriggerManager: trigger PV
    """

    instances: List["FakePV"] = []
    next_index: int = 1
    callback_mode: str = "list"  # "list" or "dict"
    track_instances: bool = False
    use_raw_pv: bool = False
    track_put_calls: bool = False
    default_get_value: Any = None

    def __init__(
        self,
        pvname: str,
        timeout: float = 1.0,
        queueing_delay: float = 0.01,
        auto_monitor: bool = False,
        **kwargs: Any,
    ) -> None:
        self.pvname = pvname
        self.timeout = timeout
        self.queueing_delay = queueing_delay
        self.auto_monitor = auto_monitor
        self.kwargs = dict(kwargs)
        self.clear_callbacks_called = False
        self.disconnected = False
        self.connected = True
        self.timestamp = 0.0
        self.severity = 0
        self.status = 0
        self._value = self.__class__.default_get_value
        self.put_calls: List[Any] = []

        if self.__class__.callback_mode == "dict":
            self.callbacks: Dict[int, Callable[..., None]] = {}
        else:
            self.callbacks: List[Callable[..., None]] = []

        if self.__class__.use_raw_pv:
            self._pv = FakeRawPV()
        else:
            self._pv = SimpleNamespace(disconnect=self._disconnect)

        if self.__class__.track_instances:
            self.__class__.instances.append(self)

    def wait_for_connection(self, timeout: Optional[float] = None) -> bool:
        return True

    def add_callback(self, callback: Callable[..., None], **kwargs: Any) -> int:
        if self.__class__.callback_mode == "dict":
            idx = self.__class__.next_index
            self.__class__.next_index += 1
            self.callbacks[idx] = callback
            return idx

        self.callbacks.append(callback)
        return len(self.callbacks)

    def remove_callback(self, idx: int) -> None:
        if isinstance(self.callbacks, dict):
            self.callbacks.pop(idx, None)
            return
        if 1 <= idx <= len(self.callbacks):
            self.callbacks.pop(idx - 1)

    def clear_callbacks(self) -> None:
        self.clear_callbacks_called = True
        if isinstance(self.callbacks, dict):
            self.callbacks.clear()
        else:
            self.callbacks.clear()

    def trigger(self, idx: int, *, value: Any = None, **kwargs: Any) -> None:
        """Invoke one stored callback in monitor-style tests."""
        if isinstance(self.callbacks, dict):
            cb = self.callbacks[idx]
        else:
            cb = self.callbacks[idx - 1]
        cb(pvname=self.pvname, value=value, **kwargs)

    def get(self, timeout: Optional[float] = None, use_monitor: bool = False) -> Any:
        return self._value

    def put(self, value: Any, timeout: Optional[float] = None) -> bool:
        self._value = value
        if self.__class__.track_put_calls:
            self.put_calls.append(value)
        self.last_written = value
        return True

    def check_pv(self) -> None:
        return None

    def _disconnect(self) -> None:
        self.disconnected = True


def make_fake_epics_module(*, pv_class: Optional[type] = None) -> ModuleType:
    """Create a minimal ``epics`` module stub backed by ``FakePV``."""
    mod = ModuleType("epics")
    mod.PV = pv_class or make_fake_epics_pv_class()
    mod.ca = SimpleNamespace(
        poll=lambda: None,
        use_initial_context=lambda: None,
    )
    return mod


def make_fake_epics_pv_class():
    """EpicsPV fake for SubscriptionManager tests."""

    class FakeEpicsPV(FakePV):
        instances: List[FakePV] = []
        track_instances = True
        callback_mode = "list"
        use_raw_pv = False
        track_put_calls = False

    return FakeEpicsPV


def make_fake_monitor_pv_class(*, start_index: int = 100):
    """Monitor-capable fake for actuator tests."""

    class FakeMonitorPV(FakePV):
        instances: List[FakePV] = []
        next_index = start_index
        track_instances = False
        callback_mode = "dict"
        use_raw_pv = True
        track_put_calls = False
        default_get_value = 0

    return FakeMonitorPV


def make_fake_trigger_pv_class():
    """Trigger fake that only records put() calls."""

    class FakeTriggerPV(FakePV):
        instances: List[FakePV] = []
        track_instances = False
        callback_mode = "list"
        use_raw_pv = False
        track_put_calls = True

    return FakeTriggerPV


class FakeMonitorProvider:
    def __init__(self) -> None:
        self.callbacks_by_pv: Dict[str, Callable[..., None]] = {}
        self.removed_pvs: List[str] = []

    def supports_monitors(self) -> bool:
        return True

    def add_monitor(self, pvname: str, user_callback=None, **kwargs: Any):
        self.callbacks_by_pv[pvname] = user_callback
        return {"pvname": pvname}

    def remove_monitor(self, pvname: str) -> None:
        self.removed_pvs.append(pvname)


class FakeNoMonitorBackend:
    def supports_monitors(self) -> bool:
        return False
