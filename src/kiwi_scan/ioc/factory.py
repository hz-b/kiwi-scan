# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING

from .controller import ScanIOCController
from .datamodels import DataPVSpec

if TYPE_CHECKING:
    from .generic_scan_ioc import GenericScanIOC
logger = logging.getLogger(__name__)


@dataclass
class GenericScanIOCOptions:
    """Options needed to construct a generic kiwi-scan IOC."""

    prefix: str = "KIWI:SCAN"
    scan_type: str = "linear"
    config_name: Optional[str] = None
    config_file: Optional[str] = None
    config_dir: Optional[str] = None
    data_dir: Optional[str] = None
    replacements: Dict[str, str] = field(default_factory=dict)
    data_pvs: List[DataPVSpec] = field(default_factory=list)
    publish_period: float = 1.0


def create_controller(options: GenericScanIOCOptions) -> ScanIOCController:
    """Create IOC from options."""
    logger.info("Creating ScanIOCController: scan_type=%s config_name=%s config_file=%s",
        options.scan_type, options.config_name, options.config_file)

    return ScanIOCController(
        scan_type=options.scan_type,
        config_name=options.config_name,
        config_file=options.config_file,
        config_dir=options.config_dir,
        data_dir=options.data_dir,
        replacements=options.replacements,
    )

def create_ioc(options: GenericScanIOCOptions) -> "GenericScanIOC":
    """Create the softIOC adapter from options.

    Importing :mod:`kiwi_scan.ioc.factory` must not require the optional
    ``pythonSoftIOC`` runtime dependency.  Therefore the softIOC adapter is
    imported lazily here, when an IOC is actually created.
    """
    from .generic_scan_ioc import GenericScanIOC

    logger.info(
        "Creating IOC prefix=%s data_pvs=%d publish_period=%.3f",
        options.prefix,
        len(options.data_pvs),
        options.publish_period,
    )

    controller = create_controller(options)
    return GenericScanIOC(
        prefix=options.prefix,
        controller=controller,
        data_pvs=options.data_pvs,
        publish_period=options.publish_period,
    )


def run_ioc(options: GenericScanIOCOptions) -> None:
    """Create and run the generic kiwi-scan soft IOC."""
    ioc = create_ioc(options)
    ioc.runIOC()
