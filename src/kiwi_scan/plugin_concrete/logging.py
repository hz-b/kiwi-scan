# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
import math
import os
import time
from typing import Dict, Any, Optional, List, Tuple

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.epics_wrapper import EpicsPV, has_alarm, alarm_info, severity_name, severity_rank
from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import register_plugin


@register_plugin("LoggingPlugin")
class LoggingPlugin(ScanPlugin):

    def __init__(
        self,
        name: str,
        parameters: Optional[Dict[str, Any]] = None,
        scan: Optional["BaseScan"] = None,
    ):
        # ------------------  defaults --------------------
        super().__init__(name, parameters, scan)
        # overwrite defaults from base
        self.DEFAULT_LOG_FILE = "logging_plugin.log"
        self._init_logging()
        self.logger.debug(f"LoggingPlugin:: parameters = {parameters}")
        p = self.parameters

        # ------------------    alarm   --------------------
        self.enable_alarm_trace = bool(p.get("enable_alarm_trace", True))
        # Build PVs from alarm_log list
        self.monitored_pvs: Dict[str, EpicsPV] = {}
        for pvname in p.get("alarm_log", []):
            try:
                self.monitored_pvs[pvname] = EpicsPV(pvname)
            except Exception as exc:
                self.logger.error("Failed to create alarm PV '%s': %s", pvname, exc)

        # ------------------ point timing -------------------
        self.enable_point_timing = bool(p.get("enable_point_timing", True))
        self._last_point_time: Optional[float] = None

        # ------------------ actuator trace -----------------
        self.enable_actuator_trace = bool(p.get("enable_actuator_trace", True))
        self.actuator_trace_names = self._resolve_actuator_trace_names(
            p.get("actuator_trace") or p.get("actuators")
        )
        self._actuator_pv_map = self._build_actuator_pv_map()
        self._last_events: Dict[str, Dict[str, Any]] = {}
        self._last_ready_state: Dict[str, Optional[bool]] = {
            name: None for name in self.actuator_trace_names
        }
        self._transition_counts: Dict[str, int] = {
            name: 0 for name in self.actuator_trace_names
        }

    # ------------------ alarm trace -----------------------

    def _collect_alarm_trace(self, idx: int) -> List[Any]:
        worst_rank = -1
        worst: Tuple[str, str, Any, Any] = ("NO_ALARM", "", None, None)

        for name, pv in self.monitored_pvs.items():
            try:
                meta = pv.get_with_metadata(use_monitor=True, full=True)
                if not meta:
                    state = "ERROR"
                    severity = None
                    status = "NO_METADATA"
                else:
                    severity = meta.get("severity", None)
                    status = meta.get("status", None)

                    if has_alarm(severity):
                        self.logger.info(
                            "%s value=%s %s alarm=%s",
                            name,
                            meta.get("value"),
                            alarm_info(status, severity),
                            has_alarm(severity),
                        )
                        state = severity
                    else:
                        state = 0

            except Exception as exc:
                self.logger.error("[%s] Failed to read PV '%s': %s", idx, name, exc)
                state = 4
                severity = None
                status = str(exc)

            if severity_rank(state) > worst_rank:
                worst_rank = severity_rank(state)
                worst = (severity_name(state), name, severity, status)

        return list(worst)

    # ------------------ point timing ----------------------

    def _collect_point_timing(self) -> List[Any]:
        now = time.time()
        point_dt = None
        if self._last_point_time is not None:
            point_dt = max(0.0, now - self._last_point_time)
        self._last_point_time = now

        return [point_dt]

    # ------------------ actuator trace --------------------

    def _resolve_actuator_trace_names(self, configured: Any) -> List[str]:
        """Return actuator names for actuator trace columns."""
        try:
            actuators = self.scan.get_actuators() if self.scan is not None else {}
        except Exception:
            actuators = {}

        if configured is None:
            return list(actuators.keys())
        if isinstance(configured, str):
            return [configured]
        if isinstance(configured, (list, tuple, set)):
            return [str(name) for name in configured]

        self.logger.warning("Ignoring invalid actuator_trace parameter: %r", configured)
        return list(actuators.keys())

    def _build_actuator_pv_map(self) -> Dict[str, Tuple[str, str]]:
        """Map configured actuator PV names to (actuator_name, source)."""
        result: Dict[str, Tuple[str, str]] = {}
        cfg = getattr(getattr(self.scan, "cfg", None), "actuators", {}) or {}

        for name in self.actuator_trace_names:
            act_cfg = cfg.get(name)
            for source, attr in (
                ("rbv", "rb_pv"),
                ("cmd", "cmd_pv"),
                ("cmd", "pv"),
                ("status", "status_pv"),
            ):
                pvname = getattr(act_cfg, attr, None) if act_cfg is not None else None
                if pvname:
                    result[str(pvname)] = (name, source)

        return result

    @staticmethod
    def _event_timestamp(ev: PvEvent) -> Optional[float]:
        if getattr(ev, "timestamp", None) is not None:
            try:
                return float(ev.timestamp)
            except (TypeError, ValueError):
                pass
        sec = getattr(ev, "posixseconds", None)
        nsec = getattr(ev, "nanoseconds", None)
        if sec is not None:
            try:
                return float(sec) + (float(nsec or 0) * 1e-9)
            except (TypeError, ValueError):
                pass
        return None

    def _record_event(self, actuator_name: str, source: str, ev: PvEvent) -> None:
        ts = self._event_timestamp(ev)
        self._last_events[f"{actuator_name}:{source}"] = {
            "value": getattr(ev, "value", None),
            "timestamp": ts,
            "arrival_time": time.time(),
            "pvname": getattr(ev, "pvname", None),
        }
    
    def _decode_ready_from_status(self, actuator_name: str, value: Any) -> Optional[bool]:
    """ Determine the ready status for transition cycle trace only. 
        This mirrors the implementation from the actuator framework
        The function is processed from a monitor event context. 
        Use is_ready() in regular ca_context"""
        try:
            actuator = self.scan.get_actuator(actuator_name)
        except Exception:
            return None

        cfg = getattr(actuator, "config", None)
        if cfg is None:
            return None

        mask = getattr(cfg, "ready_bitmask", 0)
        ready_value = getattr(cfg, "ready_value", 0)
        if value is None:
            return None

        if mask:
            try:
                status = int(value)
                ready_val = int(ready_value, 0) if isinstance(ready_value, str) else int(ready_value)
                return (status & int(mask)) == ready_val
            except (TypeError, ValueError):
                return None

        try:
            return float(value) == float(ready_value)
        except (TypeError, ValueError):
            return str(value).strip() == str(ready_value).strip()

    def _read_actuator_value(self, actuator: Any, source: str) -> Any:
        if source == "rbv":
            return actuator.rbv
        if source == "cmd":
            try:
                cmd = actuator.cmdv
            except Exception:
                cmd = None
            if cmd is not None:
                return cmd
            pv = getattr(actuator, "pv", None)
            if pv is not None and hasattr(pv, "get"):
                return pv.get(use_monitor=True)
            return None
        if source == "status":
            pv = getattr(actuator, "status_pv", None)
            if pv is not None and hasattr(pv, "get"):
                return pv.get(use_monitor=True)
            return None
        raise ValueError(f"Unsupported actuator trace source: {source}")

    def _get_or_read_actuator_value(self, actuator_name: str, source: str) -> Any:
        key = f"{actuator_name}:{source}"
        cached = self._last_events.get(key)
        if cached is not None:
            return cached.get("value")

        try:
            actuator = self.scan.get_actuator(actuator_name)
            value = self._read_actuator_value(actuator, source)
        except Exception as exc:
            self.logger.debug(
                "Failed to read actuator trace value %s:%s: %s",
                actuator_name,
                source,
                exc,
            )
            return None

        self._last_events[key] = {
            "value": value,
            "timestamp": None,
            "arrival_time": time.time(),
            "pvname": None,
        }
        return value

    def _event_age(self, actuator_name: str, source: str) -> Any:
        item = self._last_events.get(f"{actuator_name}:{source}")
        if item is None:
            return None

        event_time = item.get("timestamp") or item.get("arrival_time")
        if event_time is None:
            return None
        return max(0.0, time.time() - float(event_time))

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            f = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(f):
            return None
        return f

    def _collect_actuator_trace(self) -> List[Any]:
        values: List[Any] = []

        for name in self.actuator_trace_names:
            rbv = self._get_or_read_actuator_value(name, "rbv")
            cmd = self._get_or_read_actuator_value(name, "cmd")
            status = self._get_or_read_actuator_value(name, "status")

            ready = self._decode_ready_from_status(name, status)
            if ready is None:
                try:
                    ready = bool(self.scan.get_actuator(name).is_ready())
                except Exception:
                    ready = None

            previous_ready = self._last_ready_state.get(name)
            if previous_ready is not None and ready is not None and ready != previous_ready:
                self._transition_counts[name] = self._transition_counts.get(name, 0) + 1
                self.logger.info(
                    "[%s] actuator transition: %s -> %s",
                    name,
                    "ready" if previous_ready else "not_ready",
                    "ready" if ready else "not_ready",
                )
            if ready is not None:
                self._last_ready_state[name] = ready

            rbv_f = self._safe_float(rbv)
            cmd_f = self._safe_float(cmd)
            following_error = None
            if rbv_f is not None and cmd_f is not None:
                following_error = abs(rbv_f - cmd_f)

            values += [
                "ready" if ready is True else "not_ready" if ready is False else "unknown",
                following_error,
                rbv,
                cmd,
                status,
                self._event_age(name, "rbv"),
                self._event_age(name, "cmd"),
                self._event_age(name, "status"),
                self._transition_counts.get(name, 0),
            ]

        return values

    # ------------------ plugin hooks -------------------

    def get_values(self, idx: int, pos: Dict[str, Any]) -> List[Any]:
        values: List[Any] = []

        if self.enable_alarm_trace:
            values += self._collect_alarm_trace(idx)
        if self.enable_point_timing:
            values += self._collect_point_timing()
        if self.enable_actuator_trace:
            values += self._collect_actuator_trace()

        return values

    def get_headers(self, timestamps: bool) -> List[str]:
        hdrs: List[str] = []

        if self.enable_alarm_trace:
            hdrs += ["AlarmState", "AlarmPV", "AlarmSeverity", "AlarmStatus"]
        if self.enable_point_timing:
            hdrs += ["PointDtS"]
        if self.enable_actuator_trace:
            for name in self.actuator_trace_names:
                prefix = f"Actuator{name}"
                hdrs += [
                    f"{prefix}ReadyState",
                    f"{prefix}FE",
                    f"{prefix}RBV",
                    f"{prefix}CMD",
                    f"{prefix}Status",
                    f"{prefix}RBVAgeS",
                    f"{prefix}CMDAgeS",
                    f"{prefix}StatusAgeS",
                    f"{prefix}Transitions",
                ]

        return self.expand_headers(hdrs, timestamps)

    def on_monitor(self, ev: PvEvent) -> None:
        """Track actuator PV monitor events for age and transition diagnostics."""
        if not self.enable_actuator_trace:
            return

        pvname = str(getattr(ev, "pvname", ""))
        mapped = self._actuator_pv_map.get(pvname)
        if mapped is None:
            self.logger.debug("Ignoring non-actuator monitor event: %s", ev)
            return

        actuator_name, source = mapped
        self._record_event(actuator_name, source, ev)

        if source == "status":
            ready = self._decode_ready_from_status(actuator_name, getattr(ev, "value", None))
            previous_ready = self._last_ready_state.get(actuator_name)
            if previous_ready is not None and ready is not None and ready != previous_ready:
                self._transition_counts[actuator_name] = self._transition_counts.get(actuator_name, 0) + 1
                self.logger.info(
                    "[%s] actuator monitor transition: %s -> %s, status=%r, pv=%s",
                    actuator_name,
                    "ready" if previous_ready else "not_ready",
                    "ready" if ready else "not_ready",
                    getattr(ev, "value", None),
                    pvname,
                )
            if ready is not None:
                self._last_ready_state[actuator_name] = ready
