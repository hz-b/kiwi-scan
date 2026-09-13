# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Minimal scan interface exposed to scan plugins."""

from __future__ import annotations

from typing import Any, Dict, Protocol, runtime_checkable

from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.datamodels import ScanConfig


@runtime_checkable
class ScanPluginContext(Protocol):
    """Supported scan resources available to plugins.

    This protocol deliberately exposes only the scan configuration, completed
    and in-progress point access, and actuator access used by existing plugins.
    It is separate from ``ScanABC`` because plugin extensions and external scan
    controllers have different API requirements.
    """

    cfg: ScanConfig

    def get_value(
        self,
        name: str,
        *,
        default: Any = None,
        with_metadata: bool = False,
    ) -> Any:
        """Return a value from the last completed scan point."""
        ...

    def get_current_row_cache(self) -> Dict[str, Any]:
        """Return a defensive copy of the current in-progress scan row."""
        ...

    def get_current_row_value(self, key: str, default: Any = None) -> Any:
        """Return one value from the current in-progress scan row."""
        ...

    def get_actuator(self, name: str) -> AbstractActuator:
        """Return one configured actuator by name."""
        ...

    def get_actuators(self) -> Dict[str, AbstractActuator]:
        """Return the configured actuator mapping."""
        ...


__all__ = ["ScanPluginContext"]
