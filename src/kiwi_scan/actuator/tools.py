# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Helpers for loading configured actuator instances."""

from typing import Dict, Mapping, Optional

from kiwi_scan.actuator.factory import create_actuators
from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.yaml_loader import yaml_loader


def load_actuators(
    config_file: str,
    replacements: Optional[Mapping[str, str]] = None,
) -> Dict[str, AbstractActuator]:
    """Load actuator definitions from YAML and create actuator instances."""
    config = yaml_loader(config_file, dict(replacements or {}))
    return create_actuators(config.get("actuators") or {})
