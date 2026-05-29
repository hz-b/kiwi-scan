# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Datamodels and parsing helpers for the generic kiwi-scan IOC API."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Iterable, List, Optional


class ScanIOCStatus(IntEnum):
    """Public status values exposed as an EPICS mbbi-like record."""

    IDLE = 0
    RUNNING = 1
    INITIALIZING = 2
    ERROR = 3


_DATA_TYPE_ALIASES = {
    "float": "float",
    "double": "float",
    "ai": "float",
    "int": "int",
    "integer": "int",
    "long": "int",
    "longin": "int",
    "str": "str",
    "string": "str",
    "stringin": "str",
    "bool": "bool",
    "boolean": "bool",
    "bi": "bool",
}

@dataclass(frozen=True)
class DataPVSpec:
    """Mapping from one kiwi-scan data key to one local IOC data PV.

    Parameters
    ----------
    local_name:
        Local IOC data record suffix.
    key:
        Key passed to ``scan.get_value(key, default=...)``.
    value_type:
        One of ``float``, ``int``, ``str``, or ``bool``.
    """

    local_name: str
    key: str
    value_type: str = "float"

    def __post_init__(self) -> None:
        object.__setattr__(self, "value_type", self._normalize_type())
        object.__setattr__(self, "local_name", self._normalize_field(self.local_name, "data PV local name"))
        object.__setattr__(self, "key", self._normalize_field(self.key, "data PV key"))

    def _normalize_type(self) -> str:
        normalized = _DATA_TYPE_ALIASES.get(str(self.value_type).strip().lower())

        if normalized is None:
            allowed = ", ".join(sorted(set(_DATA_TYPE_ALIASES.values())))
            raise ValueError(f"Unsupported data PV type {self.value_type!r} for {self.local_name}. Allowed types: {allowed}")

        return normalized

    @staticmethod
    def _normalize_field(value: str, description: str) -> str:
        value = str(value).strip()

        if not value:
            raise ValueError(f"{description} must not be empty")

        return value

    def default_value(self) -> Any:
        if self.value_type == "float":
            return float("nan")
        if self.value_type == "int":
            return 0
        if self.value_type == "bool":
            return False
        return ""

    def cast(self, value: Any) -> Any:
        """Cast a scan value into the Python type expected by the IOC record."""
        if value is None:
            return self.default_value()

        if self.value_type == "float":
            try:
                return float(value)
            except (TypeError, ValueError):
                return float("nan")

        if self.value_type == "int":
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return 0

        if self.value_type == "bool":
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
            return bool(value)

        return str(value)


def parse_data_pv_spec(spec: str) -> DataPVSpec:
    """Parse from args ``LOCAL=KEY[:TYPE]`` into a :class:`DataPVSpec`.

    FIXME: EPICS PV names and kiwi-scan keys often contain colons. Therefore the final
    ``:TYPE`` suffix is only treated as a type when it matches a known type.
    """
    if not isinstance(spec, str) or not spec.strip():
        raise ValueError("data PV spec must not be empty")
    if "=" not in spec:
        raise ValueError("data PV spec must be LOCAL=KEY[:TYPE], got %r" % spec)

    local, rhs = spec.split("=", 1)
    local = local.strip()
    rhs = rhs.strip()
    if not local:
        raise ValueError("data PV spec %r has an empty LOCAL name" % spec)
    if not rhs:
        raise ValueError("data PV spec %r has an empty KEY" % spec)

    key = rhs
    value_type = "float"
    if ":" in rhs:
        maybe_key, maybe_type = rhs.rsplit(":", 1)
        normalized = _DATA_TYPE_ALIASES.get(maybe_type.strip().lower())
        if normalized is not None:
            key = maybe_key.strip()
            value_type = normalized

    return DataPVSpec(local_name=local, key=key, value_type=value_type)


def parse_data_pv_specs(specs: Optional[Iterable[str]]) -> List[DataPVSpec]:
    """
    Parse an optional --data-pv args.
    """
    return [parse_data_pv_spec(item) for item in (specs or [])]
