# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT


from __future__ import annotations

import logging
import math
from contextlib import ExitStack
from typing import Optional, TextIO

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.epics_wrapper import EpicsPV
from kiwi_scan.monitor.row_format import MonitorRowFormatter
from kiwi_scan.scan.detector_reader import create_detector_reader

logger = logging.getLogger(__name__)


def _resolve_timeouts(config, read_timeout, connection_timeout):
    if read_timeout is None:
        read_timeout = config.detector_read_timeout
    if read_timeout is None:
        read_timeout = 1.0

    if isinstance(read_timeout, bool) or isinstance(connection_timeout, bool):
        raise TypeError("Timeouts must be finite positive numbers")

    read_timeout = float(read_timeout)
    connection_timeout = float(connection_timeout)

    if not all(
        math.isfinite(value) and value > 0
        for value in (read_timeout, connection_timeout)
    ):
        raise ValueError("Timeouts must be finite positive numbers")

    return read_timeout, connection_timeout


def _connect_detector_pvs(names, connection_timeout, cleanup):
    pvs, indices = [], []
    for index, name in enumerate(names):
        try:
            pv = EpicsPV(name, auto_monitor=False, connection_timeout=connection_timeout)
            cleanup.callback(pv.close)
        except Exception as exc:  # noqa: BLE001 - isolate initialization failures
            logger.warning("Cannot initialize detector %s: %s", name, exc)
            continue

        pvs.append(pv)
        indices.append(index)

    return pvs, indices

## TODO: incluse monitor PVs, plugin processing
def print_detector_values(
    config: ScanConfig,
    *,
    read_timeout: Optional[float] = None,
    connection_timeout: float = 1.0,
    output_format: str = "tsv",
    include_timestamps: bool = False,
    out: Optional[TextIO] = None,
) -> int:
    """
    One-shot detector readout based on detector reader and monitor formatter.
    Print one direct reading per detector
    - read_timeout applies to every PV.
    - connection timeout is handles seperately
    - EPICS PV resouces are freed at cleanup
    """
    ## PVs
    if not isinstance(config.detector_pvs, (list, tuple)):
        raise TypeError("detector_pvs must be a list of PV names")
    if any(not isinstance(name, str) or not name.strip() for name in config.detector_pvs):
        raise ValueError("detector_pvs must contain nonempty PV name strings")
    ## timeouts
    read_timeout, connection_timeout = _resolve_timeouts(config, read_timeout, connection_timeout)
    ## ouput
    if output_format not in MonitorRowFormatter.SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported output format: {output_format!r}")

    # collect and run cleanup actions in reverse registration order
    with ExitStack() as cleanup:
        formatter = MonitorRowFormatter({"format": output_format, "include_timestamps": include_timestamps}, out=out)
        cleanup.callback(formatter.close)
        pvs, indices = [], []
        readings = [None] * len(config.detector_pvs) # initialize PVs for handling failed connections
        pvs, indices = _connect_detector_pvs(config.detector_pvs, connection_timeout, cleanup)
        reader = create_detector_reader("direct", pvs, use_monitor=False, read_timeout=read_timeout)
        cleanup.callback(reader.stop)
        reader.start()
        for index, reading in zip(indices, reader.read()):
            readings[index] = reading
        formatter.start(config.detector_pvs)
        formatter.write(readings)

    return int(any(reading is None for reading in readings))
