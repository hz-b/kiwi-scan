# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from queue import Empty, Full, Queue
from typing import Any, Dict, List, Optional

import numpy as np

from kiwi_scan.epics_wrapper import EpicsPV as PV

logger = logging.getLogger(__name__)

class MetadataCAMonitor:
    """
    Event-driven sidecar logger for metadata PVs.

    Fix: write an initial snapshot row for every configured PV at start(),
    so files always contain initial values even if monitors only fire on change.
    """

    def __init__(
        self,
        pvs: List[str],
        constants: Dict[str, Any],
        outfile: str,
        queue_maxsize: int = 10000,
    ) -> None:
        self._pvspecs = pvs or []
        self._constants = dict(constants or {})
        self._outfile = outfile
        self._q: Queue[Dict[str, Any]] = Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._writer_thread: Optional[threading.Thread] = None
        self._pvobjs: List[PV] = []
        self._drop_lock = threading.Lock()
        self._dropped_events = 0
        self._last_drop_warning_monotonic = 0.0

    # ---------- public API ----------
    def start(self) -> None:
        if not self._pvspecs and not self._constants:
            logger.info("MetadataCAMonitor: nothing to start (no PVs/constants).")
            return

        # 1) Write file header (constants + column names)
        self._write_header()
        logger.debug("Metadata monitor header written")

        # 2) Create PVs, install callbacks (events go to queue even before writer starts)
        self._pvobjs = []
        for name in self._pvspecs:
            try:
                pv = PV(name, auto_monitor=True)
                pv.add_callback(self._on_event)
                self._pvobjs.append(pv)
            except ConnectionError as exc:
                logger.warning(
                    "MetadataCAMonitor: skipping unavailable PV %s: %s",
                    name,
                    exc,
                )
            except Exception:
                logger.exception(
                    "MetadataCAMonitor: failed to subscribe %s",
                    name,
                )

        # 3) Write one initial snapshot row per PV at the TOP (right after header)
        #    This guarantees an initial value even when CA monitors only fire on change.
        try:
            self._write_initial_snapshot_rows()
        except Exception:
            logger.exception("MetadataCAMonitor: failed to write initial snapshot rows")

        # 4) Start writer thread for subsequent monitor events
        self._stop.clear()
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name="scan-meta-writer",
            daemon=True,
        )
        self._writer_thread.start()

        logger.info(
            "MetadataCAMonitor: started with %d PVs → %s",
            len(self._pvobjs),
            self._outfile,
        )

    def stop(self, join_timeout: float = 2.0) -> None:
        for pv in self._pvobjs:
            try:
                pv.clear_callbacks()
                pv.disconnect()
            except Exception: # noqa BLE001
                logger.debug(f"Failed to clear {pv}")
        self._pvobjs.clear()

        # stop writer
        self._stop.set()
        if self._writer_thread:
            self._writer_thread.join(timeout=join_timeout)
            self._writer_thread = None
        dropped = self.get_drop_count()
        if dropped:
            logger.warning( "MetadataCAMonitor: stopped with %d dropped queue event(s).", dropped)
        else:
            logger.debug("MetadataCAMonitor: stopped with no dropped queue events.")

    def get_drop_count(self) -> int:
        """Return the number of monitor events dropped because the queue was full."""
        with self._drop_lock:
            return self._dropped_events

    @property
    def dropped_events(self) -> int:
        """Number of queue-full drops recorded since this monitor was created."""
        return self.get_drop_count()

    # ---------- internals ----------
    def _write_header(self) -> None:
        cols = [
            "TS-ISO8601",      # wall-clock receive time (UTC)
            "PV",              # pv name
            "VALUE",           # best-effort numeric or str
            "PV-TS-ISO8601",   # PV timestamp if available
            "SEVR",            # severity if available
            "STAT",            # status if available
        ]

        with open(self._outfile, "w") as f:
            if self._constants:
                f.write("# metadata_constants\n")
                f.writelines(f"# {k}\t{v}\n" for k, v in self._constants.items())
                f.write("# --- metadata above; monitor data below ---\n")
            f.write("\t".join(cols) + "\n")

    def _write_initial_snapshot_rows(self) -> None:
        """
        Append exactly one snapshot row per PV right after the header.
        Uses the same column format as the monitor updates.
        """
        if not self._pvobjs:
            return

        with open(self._outfile, "a", encoding="utf-8") as f:
            for pv in self._pvobjs:
                pvname = pv.pvname
                pvname = pvname or "UNKNOWN"

                md = None
                try:
                    md = pv.get_with_metadata()
                except Exception: # noqa BLE001
                    logger.debug(f"Faild to get_with_metadata() pv={pv.pvname}")
                    md = None

                value = None
                ts = None
                sevr = None
                stat = None

                if isinstance(md, dict) and md:
                    value = md.get("value")
                    ts = md.get("timestamp")
                    sevr = md.get("severity")
                    stat = md.get("status")
                else:
                    value = None

                row = [
                    datetime.now(tz=timezone.utc).isoformat(),
                    pvname,
                    self._fmt_value(value),
                    self._ts_to_iso(ts),
                    self._fmt_plain(sevr),
                    self._fmt_plain(stat),
                ]
                f.write("\t".join(row) + "\n")

            f.flush()

    def _on_event(self, **kwargs) -> None:
        try:
            event = {
                "recv_ts": datetime.now(tz=timezone.utc).isoformat(),
                "pv": kwargs.get("pvname") or kwargs.get("pv") or "UNKNOWN",
                "value": kwargs.get("value"),
                "pv_ts": self._ts_to_iso(kwargs.get("timestamp")),
                "sevr": kwargs.get("severity"),
                "stat": kwargs.get("status"),
            }
            try:
                self._q.put_nowait(event)
            except Full:
                now = time.monotonic()
                with self._drop_lock:
                    self._dropped_events += 1
                    dropped = self._dropped_events
                    warn = (
                        now - self._last_drop_warning_monotonic >= 5.0
                    )
                    if warn:
                        self._last_drop_warning_monotonic = now

                if warn:
                    logger.warning(
                        "MetadataCAMonitor: queue full; dropped_events=%d "
                        "queue_size=%d queue_maxsize=%d",
                        dropped,
                        self._q.qsize(),
                        self._q.maxsize,
                    )
        except Exception:
            logger.exception("MetadataCAMonitor: callback error")

    @staticmethod
    def _ts_to_iso(ts: Any) -> str:
        try:
            if ts is not None:
                return datetime.fromtimestamp( float(ts), tz=timezone.utc,).isoformat()
            return str(ts)
        except (TypeError, ValueError, OverflowError, OSError):
            return ""

    def _writer_loop(self) -> None:
        with open(self._outfile, "a") as f:
            while not self._stop.is_set():
                try:
                    ev = self._q.get(timeout=0.25)
                except Empty:
                    continue

                row = [
                    ev.get("recv_ts", ""),
                    ev.get("pv", ""),
                    self._fmt_value(ev.get("value")),
                    ev.get("pv_ts", ""),
                    self._fmt_plain(ev.get("sevr")),
                    self._fmt_plain(ev.get("stat")),
                ]
                f.write("\t".join(row) + "\n")
                f.flush()

    @staticmethod
    def _fmt_scalar(value: Any) -> str:
        if value is None:
            return ""

        if isinstance(value, (int, float)):
            return f"{float(value):.12e}"

        return str(value)

    @classmethod
    def _fmt_value(cls, value: Any) -> str:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")

        if isinstance(value, np.ndarray):
            # A zero-dimensional array contains one scalar value.
            if value.ndim == 0:
                return cls._fmt_scalar(value.item())

            values = value.tolist()

        elif isinstance(value, Sequence) and not isinstance(
            value,
            (str, bytes, bytearray),
        ):
            values = value

        else:
            return cls._fmt_scalar(value)

        formatted_values = (cls._fmt_scalar(item) for item in values)
        return f"[{' '.join(formatted_values)}]"

    @staticmethod
    def _fmt_plain(v: Any) -> str:
        return "" if v is None else str(v)

