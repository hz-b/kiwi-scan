# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import time
import logging
from abc import ABC, abstractmethod
from typing import Callable, Optional, Any, Dict, List

from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.epics_wrapper import EpicsPV
from kiwi_scan.actuator.single import AbstractActuator, PvEvent, MonitorCallback

class EpicsActuator(AbstractActuator):
    """
    An EPICS-based actuator implementation using the EpicsPV wrapper.
    """

    def __init__(self, config: ActuatorConfig):
        super().__init__(config)
        logging.debug("Actuator PVs")

        # Wrap PVs
        self.pv = EpicsPV(config.pv) if config.pv else None
        self.rel_pv = EpicsPV(config.rel_pv) if config.rel_pv else None
        self.rb_pv = EpicsPV(config.rb_pv) if config.rb_pv else None
        self.cmd_pv = EpicsPV(config.cmd_pv) if config.cmd_pv else None
        self.cmdvel_pv = EpicsPV(config.cmdvel_pv) if config.cmdvel_pv else None
        self.start_pv = EpicsPV(config.start_pv) if config.start_pv else None
        self.stop_pv = EpicsPV(config.stop_pv) if config.stop_pv else None
        self.status_pv = EpicsPV(config.status_pv) if config.status_pv else None
        self.velocity_pv = EpicsPV(config.velocity_pv) if config.velocity_pv else None
        self.get_velocity_pv = EpicsPV(config.get_velocity_pv) if config.get_velocity_pv else None
        # pvname -> list of callback indices (pyepics returns an int id per add_callback)
        self._epics_cb_indices: Dict[str, List[int]] = {}

        logging.debug("Acuator Jog Config")
        jog_cfg = self.config.jog
        if jog_cfg:
            self.jog_velocity_pv = EpicsPV(jog_cfg.velocity_pv) if jog_cfg.velocity_pv else None
            self.jog_command_pv = EpicsPV(jog_cfg.command_pv) if jog_cfg.command_pv else None

        # Config parameters
        self.in_band = config.in_position_band
        self.dwell_time = config.dwell_time
        self.ready_value = config.ready_value
        self.ready_bitmask = config.ready_bitmask
        self.q_delay = config.queueing_delay
        self.startup_timeout = config.startup_timeout
        self.backlash = config.backlash
        self.velocity = config.velocity
        self.start_command = config.start_command

        # Validate PV connections
        self._check_pvs()

    # --------------------- monitor backend ---------------------------------
    def supports_monitors(self) -> bool:
        return True

    def add_monitor(
        self,
        pvname: str,
        user_callback: Optional[MonitorCallback] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Subscribe to EPICS PV updates (CA monitor) and route them to _dispatch_pv_update()
        so scan/subscription callbacks receive PvEvent objects.
        """
        with self._monitor_lock:
            # Create the EPICS monitor PV only once
            if pvname not in self._monitors:
                pvname_outer = pvname

                # Create a dedicated PV for monitoring with auto_monitor enabled
                if hasattr(EpicsPV, "create_monitor"):
                    mon = EpicsPV.create_monitor(
                        pvname,
                        timeout=float(kwargs.get("timeout", 1.0)),
                        queueing_delay=float(kwargs.get("queueing_delay", self.q_delay)),
                        auto_monitor=True)
                else:
                    # fallback for tests / pyepics compatible API
                    mon = EpicsPV(
                        pvname,
                        timeout=float(kwargs.get("timeout", 1.0)),
                        queueing_delay=float(kwargs.get("queueing_delay", self.q_delay)),
                        auto_monitor=True)

                def _on_ca_event(pvname=None, value=None, **kw):
                    name = pvname if pvname is not None else pvname_outer

                    # Keep any provided kw fields; also allow callers to tag source
                    if "source" not in kw:
                        kw["source"] = "epics_monitor"

                    # IMPORTANT: route through actuator dispatcher (creates PvEvent and fans out)
                    self._dispatch_pv_update(name, value, **kw)

                # EpicsPV.add_callback returns callback index from pyepics
                logging.info(f"ACT->add_monitor: Add callback to PV {pvname}")
                cb_idx = mon.add_callback(_on_ca_event, run_now=False, with_ctrlvars=False)
                self._monitors[pvname] = mon
                self._epics_cb_indices[pvname] = [cb_idx]

            # Ensure callbacks list exists even if caller only wants to "listen"
            self._monitor_callbacks.setdefault(pvname, [])

        # Optional user callback gets PvEvent
        if user_callback is not None:
            self.on_pv_event(pvname, user_callback)

        return self._monitors.get(pvname)

    def remove_monitor(self, pvname: str) -> None:
        """
        Remove CA callbacks for the monitored PV and clear bookkeeping.
        """
        # Detach CA callbacks
        with self._monitor_lock:
            mon = self._monitors.get(pvname)
            cb_indices = list(self._epics_cb_indices.get(pvname, []))

        try:
            # Our EpicsPV wraps a real pyepics.PV in mon._pv
            raw_pv = getattr(mon, "_pv", None) if mon is not None else None
            if raw_pv is not None:
                # Prefer removing only our callbacks if possible
                if hasattr(raw_pv, "remove_callback"):
                    for idx in cb_indices:
                        try:
                            raw_pv.remove_callback(idx)
                        except Exception:
                            logging.exception("[EPICS] remove_callback failed for %s idx=%r", pvname, idx)
                elif hasattr(raw_pv, "clear_callbacks"):
                    # Fallback: clear everything on that PV (coarser)
                    raw_pv.clear_callbacks()

                # Optional: disconnect to stop CA traffic
                if hasattr(raw_pv, "disconnect"):
                    try:
                        raw_pv.disconnect()
                    except Exception:
                        pass
        except Exception:
            logging.exception("[EPICS] Failed to detach monitor callbacks for %s", pvname)

        with self._monitor_lock:
            self._epics_cb_indices.pop(pvname, None)

        # Clear monitor bookkeeping (callbacks + last events)
        super().remove_monitor(pvname)

    def clear_monitors(self) -> None:
        """
        Remove all EPICS monitors for this actuator.
        """
        with self._monitor_lock:
            pvs = list(self._monitors.keys())
        for pv in pvs:
            self.remove_monitor(pv)

    def _dispatch_pv_update(self, pvname: str, value: Any, **kw: Any):
        """
        Only record/callback for PVs that are still monitored/listened to.
        """
        with self._monitor_lock:
            has_monitor = pvname in self._monitors
            has_listeners = bool(self._monitor_callbacks.get(pvname))
        if not (has_monitor or has_listeners):
            return None
        return super()._dispatch_pv_update(pvname, value, **kw)

    @property
    def pvname(self) -> str:
        return self.pv.pvname

    @property
    def rbv(self) -> Optional[Any]:
        """
        Shortcut property to get the readback value.
        """
        if self.rb_pv:
            return self.rb_pv.get()
        return None

    @rbv.setter
    def rbv(self, value: Any) -> None:
        """
        Shortcut property to set the readback value (for testing or simulation).
        """
        if self.rb_pv:
            self.rb_pv.put(value)
        else:
            raise AttributeError("Read-back PV not configured; cannot set rbv")

    @property
    def cmdv(self) -> Optional[Any]:
        """
        Shortcut property to get the commanded position value.
        """
        if self.cmd_pv:
            return self.cmd_pv.get()
        return None

    @cmdv.setter
    def cmdv(self, value: Any) -> None:
        """
        Shortcut property to set the commanded position value (for testing or simulation).
        """
        if self.cmd_pv:
            self.cmd_pv.put(value)
        else:
            raise AttributeError("Command PV not configured; cannot set cmdv")
    
    @property
    def cmdvelv(self) -> Optional[Any]:
        """
        Shortcut property to get the commanded position value.
        """
        if self.cmdvel_pv:
            return self.cmdvel_pv.get()
        return None

    @cmdvelv.setter
    def cmdvelv(self, value: Any) -> None:
        """
        Shortcut property to set the commanded position value (for testing or simulation).
        """
        if self.cmdvel_pv:
            self.cmdvel_pv.put(value)
        else:
            raise AttributeError("Command PV not configured; cannot set cmdv")
    
    def _check_pvs(self) -> None:
        """Ensure all required PVs are connected and writable."""
        if self.pv:
            self.pv.check_pv()
        else:
            logging.warning("Creating EPICS actuator without setter PV")
        if self.start_pv:
            self.start_pv.check_pv()
        if self.velocity_pv:
            self.velocity_pv.check_pv()
        if self.stop_pv:
            self.stop_pv.check_pv()

    def _wait_for_condition(
        self,
        condition: Callable[[], bool],
        timeout: Optional[float] = None,
        interval: float = 0.1,
        msg: str = "Timeout waiting for condition",
    ) -> None:
        """
        Wait until `condition()` returns True.
        If `timeout` is None, wait indefinitely. Otherwise, stop after `timeout` seconds.
        """
        start = time.time()
        while not condition():
            if timeout is not None and (time.time() - start) > timeout:
                logging.error(msg)
                return
            time.sleep(interval)

    def start_actuator(self) -> None:
        if self.start_pv:
            success = self.start_pv.put(self.start_command)
            if not success:
                logging.error(f"Failed to start actuator via {self.start_pv.pvname}")

    def set_velocity(self, velocity: float) -> None:
        self.velocity = velocity
        if self.velocity_pv:
            success = self.velocity_pv.put(velocity)
            if not success:
                logging.error(f"Failed to set velocity via {self.velocity_pv.pvname}")
        logging.info(f"Velocity set to {self.velocity}")

    def get_velocity(self) -> Optional[float]:
        if self.get_velocity_pv:
            return self.get_velocity_pv.get()
        return None

    def _issue_move(self, position: float) -> None:
        logging.info(f"[{self.pvname}] move to {position}")
        if self.pv:
            success = self.pv.put(position)
        if not success:
            logging.error(f"Failed to write position to {self.pvname}")
        self.start_actuator()
        time.sleep(self.q_delay)

    def move(self, position: float) -> None:
        self._issue_move(position)

    def _issue_rel_move(self, delta: float) -> None:
        """Issue a relative move.

        If config.rel_pv exists and is connected, write delta to it.
        Otherwise compute an absolute target from rbv and use the normal move PV.
        """
        if self.rel_pv is not None:
            logging.info(f"[{self.pvname}] rel-move by {delta}")
            success = self.rel_pv.put(delta)
            if not success:
                logging.error("Failed to write relative move %r to %s", delta, self.rel_pv.pvname)
            self.start_actuator()
            time.sleep(self.q_delay)
            return

        # Fallback: compute absolute target from readback
        cur = self.rbv
        if cur is None:
            logging.error( f"Relative move requested but no rel_pv configured/available and rbv is None for actuator '{self.pvname}'.")
            return 
        try:
            target = float(cur) + float(delta)
        except Exception as exc:
            logging.error(f"Failed to compute absolute target from rbv={cur!r} and delta={delta!r}: {exc}")
            return 
        logging.info(f"[{self.pvname}] rel-move fallback: rbv={cur} delta={delta} -> target={target}")
        self._issue_move(target)

    def run_move(self, position: float, sync: bool = True) -> None:
        self.move(position)
        if sync:
            self.wait_until_done(position)

    def run_rel_move(self, delta: float, sync: bool = True) -> None:
        """Relative move; if we fall back to absolute moves, we wait on the computed target."""
        if self.rel_pv is not None:
            cur = self.rbv
            self.rel_move(delta)
            if sync:
                try:
                    if cur is not None and self.in_band >= 0:
                        self.wait_until_done(float(cur) + float(delta))
                    else:
                        self.wait_for_startup_and_done()
                        self.dwell()
                except Exception:
                    self.wait_for_startup_and_done()
                    self.dwell()
            return

        cur = self.rbv
        if cur is None:
            logging.error( f"Relative move requested but no rel_pv configured/available and rbv is None for actuator '{self.pvname}'.")
            return
        target = float(cur) + float(delta)
        self._issue_move(target)
        if sync:
            self.wait_until_done(target)

    def rel_move(self, delta: float) -> None:
        self._issue_rel_move(delta)

    def jog(self, velocity: float, sync: bool = True) -> None:
        
        logging.info(f"Jog with velocity {velocity}")

        jog_cfg = self.config.jog
        logging.info(f"jog_cfg = {jog_cfg}")
        if not jog_cfg:
            raise ValueError("Jog feature is not configured for this actuator")
        
        # 1) If a velocity PV is provided, write the desired velocity
        if self.jog_velocity_pv:
            if jog_cfg.abs_velocity:
                cmd_velocity = abs(velocity)
            else:
                cmd_velocity = velocity
            success = self.jog_velocity_pv.put(cmd_velocity)
            if not success:
                logging.error(f"Failed to set velocity via {self.jog_velocity_pv.pvname}")
        
        # 2) If a command PV is provided, determine the command value
        if self.jog_command_pv:
            if jog_cfg.command_pos is not None and jog_cfg.command_neg is not None:
                cmd = jog_cfg.command_pos if velocity >= 0 else jog_cfg.command_neg
            else:
                # single-command mode: any nonzero velocity triggers same command
                cmd = jog_cfg.command_pos or 1.0
            success = self.jog_command_pv.put(cmd)
            if not success:
                logging.error(f"Failed to set {self.jog_command_pv.pvname}")
        if sync:
            self.wait_for_startup_and_done()
    
    def is_ready(self) -> bool:
        if not self.status_pv:
            return True

        val = self.status_pv.get()

        # -----------------------------------------------------------
        # Try bitmask logic if mask is non-zero
        # -----------------------------------------------------------
        mask = getattr(self, "ready_bitmask", 0)
        logging.debug("is_ready(): mask=%r val=%r", mask, val)
        if val is None:
            return False
        if mask:
            try:
                status = int(val)
                logging.debug(f"is_ready(): status={status}")
                mask = int(mask)
                logging.debug(f"is_ready(): mask={mask}")
                # ready_value may be int or "0x0B22"
                ready_val = int(self.ready_value, 0) if isinstance(self.ready_value, str) else int(self.ready_value)
                logging.debug(f"is_ready(): ready_val={ready_val}")
                logging.debug(f"is_ready(): mask={mask} status={status} ready_val={ready_val}")
                return (status & mask) == ready_val
            except (TypeError, ValueError):
                logging.debug(f"is_ready(): failed if mask!")
                # fall back to original logic
                pass

        # -----------------------------------------------------------
        #  Default: Simple comparison logic
        # -----------------------------------------------------------
        try:
            return float(val) == float(self.ready_value)
        except (TypeError, ValueError):
            return str(val).strip() == str(self.ready_value).strip()

    def in_position_check(self, target: float, timeout: float = 0) -> bool:
        if self.in_band < 0 or not self.rb_pv:
            return True

        start = time.time()
        while True:
            current = self.rb_pv.get()
            if current is None:
                logging.warning("Readback PV returned None")
                return True
            if abs(current - target) <= self.in_band:
                return True
            if timeout and (time.time() - start) >= timeout:
                return False
            time.sleep(0.1)

    def is_in_position(self, target, in_position_band):
        current = self.rb_pv.get()
        return abs(current - target) <= in_position_band
    
    def dwell(self) -> None:
        time.sleep(self.dwell_time)
    
    def wait_for_startup_and_done(self):
        logging.debug(f"[{self.pvname}] waiting for move to start")
        self._wait_for_condition(self.is_moving, self.startup_timeout)
        logging.debug(f"[{self.pvname}] waiting for ready state")
        self._wait_for_condition(self.is_ready)

    def wait_until_done(self, position: float) -> None:
        has_status = bool(self.status_pv)
        has_band = self.in_band >= 0
        t0 = time.time()

        if has_status and has_band:
            self.wait_for_startup_and_done()
            logging.debug(f"[{self.pvname}] waiting in-band")
            if not self.in_position_check(position):
                logging.warning(f"{self.pvname} never reached in-band position")
            self.dwell()

        elif not has_status and has_band:
            logging.debug(f"[{self.pvname}] waiting in-band only")
            if not self.in_position_check(position):
                logging.warning(f"{self.pvname} band timeout")
            self.dwell()

        elif has_status and not has_band:
            self.wait_for_startup_and_done()
            self.dwell()

        elif self.dwell_time > 0:
            logging.debug(f"[{self.pvname}] dwell only")
            self.dwell()

        else:
            logging.info(f"[{self.pvname}] no wait conditions")

        elapsed = time.time() - t0
        logging.info(f"[{self.pvname}] done in {elapsed:.3f}s")

    def stop(self) -> None:
        if self.stop_pv:
            success = self.stop_pv.put(self.config.stop_command)
            if not success:
                logging.error(f"Failed to stop actuator via {self.stop_pv.pvname}")
        else:
            logging.debug("Stop PV not defined, no action taken")

