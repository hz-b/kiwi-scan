# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import os
import time
from typing import Dict, Any, Optional, List
from kiwi_scan.epics_wrapper import EpicsPV, has_alarm, alarm_info, severity_name, severity_rank
from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import register_plugin, PluginConfig 

@register_plugin("LoggingPlugin")
class LoggingPlugin(ScanPlugin):
    
    def __init__(self, 
                 name: str, 
                 parameters: Optional[Dict[str, Any]] = None,
                 scan: Optional["BaseScan"] = None):
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

    def _collect_alarm_trace(self, idx: int) -> List[Any]:
        worst_rank = -1
        worst: Tuple[str, str, Any, Any] = ("NO_ALARM", "", None, None)
        
        index = 1; 
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
                    index = index + 1

            except Exception as exc:
                self.logger.error("[%s] Failed to read PV '%s': %s", idx, name, exc)
                state = 4
                severity = None
                status = str(exc)

            if severity_rank(state) > worst_rank:
                worst_rank = severity_rank(state)
                worst = (severity_name(state), name, severity, status)

        return list(worst)
    
    def _collect_point_timing(self) -> List[Any]:
        now = time.time()
        point_dt = None
        if self._last_point_time is not None:
            point_dt = max(0.0, now - self._last_point_time)
        self._last_point_time = now

        return [ point_dt ]

    ## TODO: actuator: not ready/ready decoded, FE abs(RBV - CMD), rbv, cmd age
    def get_values(self, idx: int, pos: Dict[str, Any]) -> List[Any]:
        
        values: List[Any] = []
        
        if self.enable_alarm_trace:
            values += self._collect_alarm_trace(idx)
        if self.enable_point_timing:
            values += self._collect_point_timing()
        
        return values

    def get_headers(self, timestamps: bool) -> List[str]:
        
        hdrs: List[str] = []

        if self.enable_alarm_trace:
            hdrs += ["AlarmState", "AlarmPV", "AlarmSeverity", "AlarmStatus"]
        if self.enable_point_timing:
            hdrs += [ "PointDtS" ]
        
        return self.expand_headers(hdrs, timestamps)
    
    ## TODO: count actuator transition changes, status age 
    #def on_monitor(self, ev: PvEvent) -> None:
