# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import threading
import time
from datetime import datetime, timezone
from queue import Queue, Full, Empty
from typing import Dict, Any, List, Optional
from kiwi_scan.epics_wrapper import EpicsPV as PV

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
        self._q: "Queue[Dict[str, Any]]" = Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._writer_thread: Optional[threading.Thread] = None
        self._pvobjs: List[PV] = []

    # ---------- public API ----------
    def start(self) -> None:
        if not self._pvspecs and not self._constants:
            logging.info("MetadataCAMonitor: nothing to start (no PVs/constants).")
            return

        # 1) Write file header (constants + column names)
        self._write_header()
        logging.debug("Metadata monitor header written")

        # 2) Create PVs, install callbacks (events go to queue even before writer starts)
        self._pvobjs = []
        for name in self._pvspecs:
            try:
                pv = PV(name, auto_monitor=True)
                pv.add_callback(self._on_event)
                self._pvobjs.append(pv)
            except Exception as e:
                logging.error(
                    "MetadataCAMonitor: failed to subscribe %s: %s",
                    name, e, exc_info=True
                )

        # 3) Write one initial snapshot row per PV at the TOP (right after header)
        #    This guarantees an initial value even when CA monitors only fire on change.
        try:
            self._write_initial_snapshot_rows()
        except Exception:
            logging.exception("MetadataCAMonitor: failed to write initial snapshot rows")

        # 4) Start writer thread for subsequent monitor events
        self._stop.clear()
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name="scan-meta-writer",
            daemon=True,
        )
        self._writer_thread.start()

        logging.info(
            "MetadataCAMonitor: started with %d PVs → %s",
            len(self._pvobjs),
            self._outfile,
        )

    def stop(self, join_timeout: float = 2.0) -> None:
        # detach callbacks / close PVs (handle EpicsPV wrapper + raw pyepics.PV)
        for pv in self._pvobjs:
            try:
                raw = getattr(pv, "_pv", pv)  # EpicsPV wrapper stores pyepics PV in ._pv
                if hasattr(raw, "clear_callbacks"):
                    raw.clear_callbacks()
                if hasattr(raw, "disconnect"):
                    raw.disconnect()
            except Exception:
                pass
        self._pvobjs.clear()

        # stop writer
        self._stop.set()
        if self._writer_thread:
            self._writer_thread.join(timeout=join_timeout)
            self._writer_thread = None
        logging.info("MetadataCAMonitor: stopped.")

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
                for k, v in self._constants.items():
                    f.write(f"# {k}\t{v}\n")
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
                pvname = getattr(pv, "pvname", None)
                if not pvname:
                    raw = getattr(pv, "_pv", None)
                    pvname = getattr(raw, "pvname", None) if raw is not None else None
                pvname = pvname or "UNKNOWN"

                md = None
                try:
                    if hasattr(pv, "get_with_metadata"):
                        md = pv.get_with_metadata()
                except Exception:
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
                    # fallback: at least try to get a value
                    try:
                        if hasattr(pv, "get"):
                            value = pv.get()
                    except Exception:
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
                if (int(time.time()) % 5) == 0:
                    logging.warning("MetadataCAMonitor: queue full, dropping events.")
        except Exception as e:
            logging.error("MetadataCAMonitor: callback error: %s", e, exc_info=True)

    @staticmethod
    def _ts_to_iso(ts: Any) -> str:
        try:
            if ts is None:
                return ""
            if isinstance(ts, (int, float)):
                return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
            return str(ts)
        except Exception:
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
    def _fmt_value(v: Any) -> str:
        if isinstance(v, (int, float)):
            return f"{float(v):.12e}"

        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", "replace")
            except Exception:
                return repr(v)

        seq = None
        try:
            import numpy as np  # type: ignore
            if isinstance(v, np.ndarray):
                seq = v.tolist()
        except Exception:
            pass

        if seq is None:
            from collections.abc import Sequence
            if isinstance(v, Sequence) and not isinstance(v, (str, bytes, bytearray)):
                seq = list(v)

        if seq is not None:
            parts = []
            for item in seq:
                if isinstance(item, (int, float)):
                    parts.append(f"{float(item):.12e}")
                else:
                    parts.append(str(item))
            return "[" + " ".join(parts) + "]"

        return "" if v is None else str(v)

    @staticmethod
    def _fmt_plain(v: Any) -> str:
        return "" if v is None else str(v)

