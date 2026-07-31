# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

""" Abstract export model for kiwi-scan conversion tools.  """

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from kiwi_scan.data.metadata_loader import MetadataFile

logger = logging.getLogger(__name__)


@dataclass
class ExportScan:
    """ One scan entry ready for export.

    Parameters:
    -----------
    scan_id:
        Stable scan identifier from the manifest, if available.
    scan_type:
        kiwi-scan scan type, for example ``LinearScan`` or ``CMScan``.
    data_file:
        Source scan data file.
    metadata_file:
        Source metadata sidecar file, if available.
    data:
        Scan data dataframe.
    metadata:
        Parsed metadata sidecar, if available and requested.
    created_at:
        Scan creation timestamp from the manifest.
    manifest_entry:
        Original manifest entry.
    """

    scan_id: Optional[str]
    scan_type: Optional[str]
    data_file: Optional[Path]
    metadata_file: Optional[Path]
    data: pd.DataFrame
    metadata: Optional[MetadataFile] = None
    created_at: Optional[datetime] = None
    manifest_entry: Dict[str, Any] = field(default_factory=dict)

    @property
    def label(self) -> str:
        """ Return a label: scan_id or filename without extension"""
        if self.scan_id:
            label = str(self.scan_id)
        elif self.data_file is not None:
            label = self.data_file.stem
        else:
            label = "scan"
        logger.debug("Resolved export scan label=%r scan_id=%r data_file=%s", label, self.scan_id, self.data_file)
        return label


@dataclass
class ExportBundle:
    """A collection of scans to be written by one format writer."""

    scans: List[ExportScan]
    manifest_file: Optional[Path] = None
    manifest_header: Dict[str, Any] = field(default_factory=dict)

    def require_scans(self) -> None:
        """Raise ``ValueError`` if the bundle is empty."""
        scan_count = len(self.scans)
        logger.debug(
            "Validating export bundle scans=%d manifest_file=%s manifest_header_keys=%s",
            scan_count,
            self.manifest_file,
            sorted(self.manifest_header.keys()),
        )
        if not self.scans:
            raise ValueError("Export bundle contains no scans")
