# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.monitor_concrete.print import PrintMonitor

logger = logging.getLogger(__name__)

# 
# Import plotting monitor lazily so tkinter/matplotlib are not
# imported at module import time for headless or non-plotting use.
#

def _create_print_monitor(parameters=None):
    return PrintMonitor(parameters=parameters)

# Plotting 
def _create_plot_monitor(parameters=None):
    from kiwi_scan.monitor_concrete.queue_plotter import QueuePlotterMonitor
    return QueuePlotterMonitor(parameters=parameters)


MONITOR_TYPES = {
    "print": _create_print_monitor,
    "plot": _create_plot_monitor,
}


def create_monitor(config: ScanConfig):
    monitor_type = config.monitor_type
    logger.debug("Creating monitor from scan config: monitor_type=%r", monitor_type)

    if monitor_type is None:
        logger.debug("No monitor_type configured; no monitor will be created")
        return None

    factory = MONITOR_TYPES.get(monitor_type)
    if factory is None:
        logger.info("Unknown monitor type: %r", monitor_type)
        return None

    monitor_config = getattr(config, "monitor", None)
    parameters = getattr(monitor_config, "parameters", {}) or {}
    if set(parameters.keys()) == {"parameters"} and isinstance(parameters.get("parameters"), dict):
        logger.debug("Unwrapping nested monitor parameters block: %r", parameters)
        parameters = parameters["parameters"]
    logger.debug(
        "Instantiating monitor type=%r with parameters=%r",
        monitor_type,
        parameters,
    )

    return factory(parameters=parameters)
