# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from typing import TypedDict, Dict, Any, Optional 

PLUGIN_REGISTRY = {}

class PluginConfig(TypedDict):
    type: str
    name: str
    parameters: Dict[str, Any]      # values untyped

def register_plugin(name=None):
    def decorator(cls):
        key = name or cls.__name__
        if key in PLUGIN_REGISTRY:
            raise ValueError(f"Plugin '{key}' already registered")
        PLUGIN_REGISTRY[key] = cls
        return cls
    return decorator

def create_plugin(
        config: PluginConfig = None,
        scan: Optional["BaseScan"] = None):

    plugin_type = config["type"]
    cls = PLUGIN_REGISTRY.get(plugin_type)
    if cls is None:
        raise ValueError(f"Unknown plugin type '{plugin_type}'")
    return cls(
        name=config.get("name", plugin_type),
        parameters=config.get("parameters", {}),
        scan=scan)
