# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import time
import logging
import threading
from typing import Optional, Any, Callable, Dict, List, Tuple

from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.actuator.single import AbstractActuator


class VirtualEventBus:
    """A tiny in-process "PV" event bus for simulation.

    This is meant to back the monitor/callback feature in the
    :class:`~scan.actuator_concrete.single_simulation.SimulatedActuator`.

    Semantics:
      * ``subscribe(pvname, cb)`` registers a callback like a pyepics callback:
        ``cb(pvname=..., value=..., **kwargs)``.
      * ``publish(pvname, value, **kwargs)`` calls all subscribers.
      * ``unsubscribe(token)`` removes a subscription.

    It is intentionally minimal: only what we need for unit tests and
    for simple simulated integrations.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._next_token = 1
        self._subs: Dict[str, List[Tuple[int, Callable[..., None]]]] = {}

    def subscribe(self, pvname: str, cb: Callable[..., None]) -> int:
        if not callable(cb):
            raise TypeError("callback must be callable")
        with self._lock:
            token = self._next_token
            self._next_token += 1
            self._subs.setdefault(pvname, []).append((token, cb))
            return token

    def unsubscribe(self, token: int) -> None:
        with self._lock:
            for pv, lst in list(self._subs.items()):
                new_lst = [(t, cb) for (t, cb) in lst if t != token]
                if new_lst:
                    self._subs[pv] = new_lst
                else:
                    self._subs.pop(pv, None)

    def publish(self, pvname: str, value: Any, **kwargs: Any) -> None:
        with self._lock:
            callbacks = [cb for (_t, cb) in self._subs.get(pvname, [])]
        for cb in callbacks:
            try:
                cb(pvname=pvname, value=value, **kwargs)
            except TypeError:
                cb(pvname, value)
            except Exception:
                logging.exception(f"[SIM BUS] subscriber failed for {pvname}")


class _SimMonitorHandle:
    """Lightweight handle for one subscription on the VirtualEventBus."""

    def __init__(self, pvname: str, bus: VirtualEventBus, token: int):
        self.pvname = pvname
        self._bus = bus
        self._token = token
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._bus.unsubscribe(self._token)
        self._closed = True


class SimulatedActuator(AbstractActuator):
    """
    A simple simulation of an actuator, using internal state
    rather than EPICS PVs.
    """

    def __init__(self, config: ActuatorConfig):
        super().__init__(config)

        # Virtual event bus for PV monitoring 
        # Allow passing in a shared bus via config.event_bus.
        self._bus: VirtualEventBus = getattr(config, "event_bus", None) or VirtualEventBus()

        # pvname -> bus subscription token (used to unsubscribe on remove)
        self._bus_tokens: Dict[str, int] = {}

        # Internal state
        self._rbv: float = getattr(config, 'initial_position', 0.0)
        self._cmdv: float = self._rbv
        self._velocity: float = config.velocity or 1000.0
        self._moving: bool = False

        # --- Periodic PV generators (heartbeat, etc.) ----------------------
        # pvname -> (thread, stop_event)
        self._pv_generators: Dict[str, Tuple[threading.Thread, threading.Event]] = {}
        self._pv_gen_lock = threading.RLock()

    # ---------------- public monitor backend ---------------------------------

    def supports_monitors(self) -> bool:
        return True

    def add_monitor(
        self,
        pvname: str,
        user_callback: Optional[Callable[..., None]] = None,
        **kwargs: Any,
    ) -> Any:
        """Attach a simulated monitor to an arbitrary PV name.

        For simulation: class:`VirtualEventBus`.
        When the bus publishes updates for *pvname*, we route them into the
        actuator-level dispatcher so user callbacks receive a PvEvent.
        """

        # 1) Create a bus subscription (once)
        with self._monitor_lock:
            if pvname not in self._monitors:
                pvname_outer = pvname

                # The bus callback signature mimics pyepics: (pvname, value, **kwargs)
                def _bus_cb(pvname=None, value=None, **kw):
                    name = pvname if pvname is not None else pvname_outer
                    self._dispatch_pv_update(name, value, **kw)

                token = self._bus.subscribe(pvname, _bus_cb)
                self._bus_tokens[pvname] = token
                self._monitors[pvname] = _SimMonitorHandle(pvname, self._bus, token)

            # Ensure there is a callbacks list for this PV
            self._monitor_callbacks.setdefault(pvname, [])

        # 2) Register optional user callback
        if user_callback is not None:
            self.on_pv_event(pvname, user_callback)

        return self._monitors.get(pvname)

    def remove_monitor(self, pvname: str) -> None:
        # Also stop any periodic generator for this PV
        self.stop_pv_generator(pvname)

        # Unsubscribe from bus first (if any)
        with self._monitor_lock:
            handle = self._monitors.get(pvname)

        if handle is not None and hasattr(handle, "close"):
            try:
                handle.close()
            except Exception:
                logging.exception(f"[SIM] failed to close monitor for {pvname}")

        with self._monitor_lock:
            self._bus_tokens.pop(pvname, None)

        # Clear monitor bookkeeping (callbacks + last events)
        super().remove_monitor(pvname)

    def _dispatch_pv_update(self, pvname: str, value: Any, **kw: Any):
        """Only record/callback for PVs that are still monitored/listened to."""
        with self._monitor_lock:
            has_monitor = pvname in self._monitors
            has_listeners = bool(self._monitor_callbacks.get(pvname))
        if not (has_monitor or has_listeners):
            return None
        return super()._dispatch_pv_update(pvname, value, **kw)

    def publish_pv(self, pvname: str, value: Any, **kwargs: Any) -> None:
        """Publish a PV update on the simulation bus."""
        if "timestamp" not in kwargs:
            kwargs["timestamp"] = time.time()
        self._bus.publish(pvname, value, **kwargs)

    # --------------------- periodic PV generator ----------------------------

    def start_pv_generator(
        self,
        pvname: str,
        rate_hz: float,
        *,
        value: Any = None,
        value_fn: Optional[Callable[[], Any]] = None,
        include_counter: bool = False,
    ) -> None:
        """Periodically publish updates for *pvname* at *rate_hz*.

        This is useful for simulated heartbeat PVs or any periodic record.

        Args:
            pvname: Name of the simulated PV to publish on.
            rate_hz: Publish rate in Hz (> 0).
            value: Constant value to publish (ignored if value_fn is provided).
            value_fn: Callable returning the next value to publish.
            include_counter: If True, include a monotonically increasing
                ``counter`` field in the published kwargs. If neither *value*
                nor *value_fn* is given, the published *value* defaults to
                the counter.
        """
        if rate_hz <= 0:
            raise ValueError("rate_hz must be > 0")

        # Replace existing generator if present
        self.stop_pv_generator(pvname)

        stop_ev = threading.Event()
        period = 1.0 / float(rate_hz)

        def _run() -> None:
            next_t = time.monotonic()
            counter = 0
            while not stop_ev.is_set():
                now = time.monotonic()
                # Sleep until the next tick (Event.wait allows prompt stop)
                if now < next_t:
                    stop_ev.wait(next_t - now)
                    continue

                # If we're behind (e.g., long callback), skip missed ticks
                while next_t <= now:
                    next_t += period

                # Compute value
                if value_fn is not None:
                    v = value_fn()
                elif value is not None:
                    v = value
                else:
                    v = counter if include_counter else 1

                kw = {}
                if include_counter:
                    kw["counter"] = counter
                kw["source"] = "sim_generator"

                try:
                    self.publish_pv(pvname, v, **kw)
                except Exception:
                    logging.exception(f"[SIM] generator publish failed for {pvname}")

                counter += 1

        th = threading.Thread(target=_run, name=f"SimPVGen:{pvname}", daemon=True)
        with self._pv_gen_lock:
            self._pv_generators[pvname] = (th, stop_ev)
        th.start()

    def stop_pv_generator(self, pvname: str) -> None:
        """Stop a running PV generator for *pvname* (if any)."""
        with self._pv_gen_lock:
            entry = self._pv_generators.pop(pvname, None)
        if not entry:
            return
        th, stop_ev = entry
        stop_ev.set()
        # Don't block hard; just give it a brief chance to exit
        th.join(timeout=0.2)

    def stop_all_pv_generators(self) -> None:
        """Stop all running PV generators."""
        with self._pv_gen_lock:
            pvs = list(self._pv_generators.keys())
        for pv in pvs:
            self.stop_pv_generator(pv)

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
        cmd_pv = self.config.cmd_pv or self.config.pv
        if cmd_pv:
            self.publish_pv(cmd_pv, self._cmdv, source="sim_cmd")

    def rel_move(self, delta: float) -> None:
        target = float(self.rbv or 0.0) + float(delta)
        self.move(target)

    def run_move(self, position: float, sync: bool = True) -> None:
        """Move and optionally wait until done."""
        self.move(position)
        if sync:
            self.wait_until_done(position)

    def run_rel_move(self, delta: float, sync: bool = True) -> None:
        target = float(self.rbv or 0.0) + float(delta)
        self.move(target)
        if sync:
            self.wait_until_done(target)

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
        return not self._moving

    def is_in_position(self, target: float, in_position_band: float) -> bool:
        return abs(self._rbv - target) <= in_position_band

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
        # ---- publish RBV update for monitors/subscriptions ----
        rbv_pv = self.config.rb_pv or self.config.pv
        if rbv_pv:
            self.publish_pv(rbv_pv, self._rbv, source="sim_motion")

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

