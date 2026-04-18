# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import os
from typing import Dict, Any, Optional, List
from epics import PV
from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import register_plugin, PluginConfig 

@register_plugin("LoggingPlugin")
class LoggingPlugin(ScanPlugin):
    
    def __init__(self, 
                 name: str, 
                 parameters: Optional[Dict[str, Any]] = None,
                 scan: Optional["BaseScan"] = None):

        super().__init__(name, parameters, scan)
        # overwrite defaults from base
        self.DEFAULT_SAMPLE_TIME = 1.0
        self.DEFAULT_LOG_FILE = "logging_plugin.log"
        self._init_logging()
        logging.debug(f"LoggingPlugin:: parameters = {parameters}")

        # Build PVs from alarm_log list
        self.monitored_pvs = {}
        alarm_list = parameters.get("alarm_log", [])
        for pvname in alarm_list:
            self.monitored_pvs[pvname] = PV(pvname)

    def get_values(self, idx: int, pos: Dict[str, Any]) -> List[Any]:
        for name, pv in self.monitored_pvs.items():
            try:
                meta = pv.get_with_metadata(timeout=1.0)
                severity = meta.get('severity', None)
                status = meta.get('status', 'UNKNOWN')

                if severity == "MAJOR":
                    self.logger.warning(
                        f"[{idx}] MAJOR ALARM detected on PV '{name}': status={status}"
                    )
                    return ["MAJOR"]
                if severity == "MINOR":
                    return ["MINOR"]
                if severity == "INVALID":
                    return ["INVALID"]

            except Exception as e:
                self.logger.error(f"[{idx}] Failed to read PV '{name}': {e}")

        self.logger.debug(f"Scan point {idx}: positions = {pos}")
        return ["NO_ALARM"]

    def get_headers(self, timestamps: bool) -> List[str]:
        hdrs = ['AlarmLogging']
        return self.expand_headers(hdrs, timestamps)

