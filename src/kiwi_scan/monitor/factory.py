# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.monitor_concrete.print import PrintMonitor
# 
# Import plotting monitor lazily so tkinter/matplotlib are not
# imported at module import time for headless or non-plotting use.
#

def _create_print_monitor():
    return PrintMonitor()

# Plotting 
def _create_plot_monitor():
    from kiwi_scan.monitor_concrete.queue_plotter import QueuePlotterMonitor
    return QueuePlotterMonitor()


MONITOR_TYPES = {
    "print": _create_print_monitor,
    "plot": _create_plot_monitor,
}


def create_monitor(config: ScanConfig):
    monitor_type = config.monitor_type
    logging.debug(f"config: {config}")

    factory = MONITOR_TYPES.get(monitor_type)
    if factory is None:
        logging.info(f"Unknown monitor type: {monitor_type!r}")
        return None

    return factory()
