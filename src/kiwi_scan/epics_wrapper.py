# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations
import logging
import threading
import time
from typing import Any, Callable, Optional, Dict
import epics

_CA_LOCK = threading.Lock()

def _safe_poll() -> None:
    """ 
    try: Yield to EPICS CA
    Best-effort; varies across pyepics versions.
    """
    try:
        epics.ca.poll()
    except Exception:
        pass

class EpicsPV:
    """
    Safe EPICS PV wrapper.
    Some CA-touching calls are serialized with _CA_LOCK to avoid segfaults from EPICS base.
    """
    def __init__(
        self,
        pvname: str,
        timeout: float = 1.0,
        queueing_delay: float = 0.01,
        auto_monitor: bool = False,
        callback: Optional[Callable[..., None]] = None,
        connection_timeout: Optional[float] = None,
    ):
        if queueing_delay < 0:
            raise ValueError(f"queueing_delay must be non-negative, got {queueing_delay}")

        self.pvname = pvname
        self.timeout = float(timeout)
        self.queueing_delay = float(queueing_delay)
        self.connection_timeout = float(connection_timeout) if connection_timeout is not None else max(10.0, self.timeout)

        self._pv: Optional[epics.PV] = None
        self._callback_refs: list[Callable[..., None]] = []  # prevent callback GC
        self.last_written: Any = None

        logging.debug("Creating PV %s", pvname)

        # Create + connect (serialized)
        def _create_and_connect() -> bool:
            try:
                pv = epics.PV(pvname, auto_monitor=False, connection_timeout=self.connection_timeout)
            except TypeError:
                pv = epics.PV(pvname, auto_monitor=False)

            self._pv = pv
            logging.debug("Wait for connection to %s", pvname)
            ok = pv.wait_for_connection(timeout=self.connection_timeout)
            _safe_poll()
            return ok

        ok = self._ca(_create_and_connect)

        if not ok:
            raise ConnectionError(f"EPICS PV '{pvname}' did not connect within {self.connection_timeout:.2f}s")

        # Enable monitoring AFTER connection (serialized)
        if auto_monitor:
            self._ca(self._enable_monitoring)

        # Optional callback
        if callback and auto_monitor:
            self.add_callback(callback, run_now=False)

    # ----------------- internal helpers -----------------

    def _ca(self, fn: Callable[[], Any]) -> Any:
        """Run a function while holding the global CA lock."""
        with _CA_LOCK:
            return fn()

    def _require_pv(self) -> epics.PV:
        if self._pv is None:
            raise RuntimeError(f"PV '{self.pvname}' not initialized")
        return self._pv

    def _enable_monitoring(self) -> None:
        pv = self._require_pv()
        try:
            pv.auto_monitor = True
        except Exception:
            # Some versions may not like setting auto_monitor after creation;
            # keep best-effort and rely on direct gets.
            pass
        _safe_poll()

    def _wrap_callback(self, user_cb: Callable[..., None]) -> Callable[..., None]:
        def _cb(pvname=None, value=None, **kwargs):
            try:
                user_cb(pvname=pvname, value=value, **kwargs)
            except TypeError:
                user_cb(pvname, value)
            except Exception:
                logging.exception("Error in PV callback for '%s'", self.pvname)
        return _cb

    # ----------------- public API -----------------

    def get(self, *, use_monitor: bool = False, timeout: Optional[float] = None) -> Any:
        """Safe get. If use_monitor=True but cache isn’t ready, falls back to direct get."""
        pv = self._require_pv()
        t = min(self.timeout, 0.2) if timeout is None else float(timeout)

        def _do_get() -> Any:
            """ Thread safe usage: self._ca(_do_get) """
            # First attempt (requested mode)
            val = pv.get(timeout=t, use_monitor=use_monitor)
            # Fallback: monitor cache may not be primed yet
            if val is None and use_monitor:
                val = pv.get(timeout=t, use_monitor=False)
            return val
        
        return _do_get()

    def get_with_metadata(self, *, use_monitor: bool = False, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Return dict with value + timestamp-ish metadata."""
        pv = self._require_pv()
        t = min(self.timeout, 0.2) if timeout is None else float(timeout)

        def _do() -> Optional[Dict[str, Any]]:
            """ Thread safe usage: self._ca(_do) """
            val = pv.get(timeout=t, use_monitor=use_monitor)
            if val is None and use_monitor:
                val = pv.get(timeout=t, use_monitor=False)
            if val is None:
                return None
            # pyepics has pv.timestamp, pv.severity, pv.status sometimes
            meta: Dict[str, Any] = {"value": val}
            try:
                meta["timestamp"] = float(getattr(pv, "timestamp"))
            except Exception:
                meta["timestamp"] = time.time()
            meta["pvname"] = self.pvname
            return meta

        return _do()

    def put(self, value: Any) -> bool:
        """Put a value to the PV.
        Supports scalar writes and waveform (array) writes.
        """
        pv = self._require_pv()

        # Normalization (e.g. numpy arrays)
        v = value
        tolist = getattr(v, "tolist", None)
        if callable(tolist):
            try:
                v = tolist()
            except Exception:
                v = value
        try:
            # TODO: Check risk and evaluate if 
            # PV puts should lock globally or per PV because of 
            # pyepics / CA thread-safety issues under load
            pv.put(v, timeout=self.timeout)
            if self.queueing_delay > 0:
                time.sleep(self.queueing_delay)
            logging.debug("Set PV %s = %r", self.pvname, v)
            self.last_written = v
            return True
        except Exception as e:
            logging.error("Failed to set PV %s to %r: %s", self.pvname, v, e)
            return False
    def add_callback(self, callback: Callable[..., None], **kwargs: Any) -> Optional[int]:
        pv = self._require_pv()
        wrapped = self._wrap_callback(callback)
        self._callback_refs.append(wrapped)

        cb_index: Optional[int] = None

        def _do_add() -> None:
            # Ensure monitoring (best effort)
            try:
                pv.auto_monitor = True
            except Exception:
                pass
            _safe_poll()
            nonlocal cb_index
            try:
                cb_index = pv.add_callback(wrapped, **kwargs)
            except TypeError:
                # Some versions don’t accept run_now, etc.
                kwargs.pop("run_now", None)
                kwargs.pop("with_ctrlvars", None)
                cb_index = pv.add_callback(wrapped, **kwargs)
            _safe_poll()

        self._ca(_do_add)

    def clear_callbacks(self) -> None:
        pv = self._require_pv()

        def _do_clear() -> None:
            try:
                pv.clear_callbacks()
            except Exception:
                pass
            _safe_poll()

        self._ca(_do_clear)
        self._callback_refs.clear()

    def check_pv(self) -> None:
        if not self._pv:
            raise RuntimeError(f"PV '{self.pvname}' not initialized")
        ok = self._ca(lambda: bool(getattr(self._pv, "connected", False)))
        if not ok:
            raise ConnectionError(f"PV '{self.pvname}' not connected")
    
    # Ignore strict connection handling above
    @classmethod
    def create_monitor(cls, pvname: str, **kwargs) -> "EpicsPV":
        """
        Create a PV for monitoring without blocking on connection.
        """
        obj = cls.__new__(cls)  # bypass __init__

        obj.pvname = pvname
        obj.timeout = float(kwargs.get("timeout", 1.0))
        obj.queueing_delay = float(kwargs.get("queueing_delay", 0.01))
        obj.connection_timeout = float(kwargs.get("connection_timeout", 10.0))
        obj._callback_refs = []
        obj.last_written = None

        # create PV WITHOUT waiting
        obj._pv = epics.PV(pvname, auto_monitor=True)

        return obj
