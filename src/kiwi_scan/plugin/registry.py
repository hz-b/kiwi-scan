# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

from typing import Optional

from kiwi_scan.datamodels import PluginConfig
from kiwi_scan.plugin.context import ScanPluginContext

PLUGIN_REGISTRY = {}


def register_plugin(name=None):
    def decorator(cls):
        key = name or cls.__name__
        if key in PLUGIN_REGISTRY:
            raise ValueError(f"Plugin '{key}' already registered")
        PLUGIN_REGISTRY[key] = cls
        return cls

    return decorator


def create_plugin(
    config: PluginConfig,
    scan: Optional[ScanPluginContext] = None,
):
    """Create a registered plugin from a normalized PluginConfig."""
    if not isinstance(config, PluginConfig):
        raise TypeError(
            "create_plugin() requires PluginConfig; normalize raw mappings "
            "through ScanConfig.from_dict() first"
        )
    plugin_type = config.type
    cls = PLUGIN_REGISTRY.get(plugin_type)
    if cls is None:
        raise ValueError(f"Unknown plugin type '{plugin_type}'")

    return cls(
        name=config.name if config.name is not None else plugin_type,
        parameters=config.parameters,
        scan=scan,
    )
