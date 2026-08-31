from typing import Any, Dict

import pytest

from kiwi_scan.datamodels import ScanConfig


def _scan_config(plugin_configs: Any) -> ScanConfig:
    return ScanConfig.from_dict({
        "actuators": {},
        "detector_pvs": [],
        "plugin_configs": plugin_configs,
    })


def test_scan_config_parses_plugins_to_typed_objects_and_preserves_custom_parameters() -> None:
    custom_parameters: Dict[str, Any] = {
        "sample_time": 0.01,
        "axis": {
            "type": "epics",
            "pv": "TEST:AXIS",
            "custom_backend_option": {"mode": "fast"},
        },
    }

    config = _scan_config([
        {
            "type": "custom.plugin",
            "name": "controller",
            "parameters": custom_parameters,
        }
    ])

    plugin_config = config.plugin_configs[0]
    assert not isinstance(plugin_config, dict)
    assert getattr(plugin_config, "type", None) == "custom.plugin"
    assert getattr(plugin_config, "name", None) == "controller"
    assert getattr(plugin_config, "parameters", None) == custom_parameters


def test_scan_config_rejects_plugin_without_type() -> None:
    with pytest.raises(ValueError, match=r"plugin_configs\[0\].*type"):
        _scan_config([{"name": "broken", "parameters": {}}])


def test_scan_config_rejects_non_mapping_plugin_parameters() -> None:
    with pytest.raises(ValueError, match=r"plugin_configs\[0\].*parameters"):
        _scan_config([{"type": "broken", "parameters": [1, 2, 3]}])


def test_create_plugin_consumes_typed_plugin_config() -> None:
    from kiwi_scan.datamodels import PluginConfig
    from kiwi_scan.plugin.registry import (
        PLUGIN_REGISTRY,
        create_plugin,
        register_plugin,
    )

    saved = dict(PLUGIN_REGISTRY)
    PLUGIN_REGISTRY.clear()
    try:
        @register_plugin("typed-demo")
        class DemoPlugin:
            def __init__(self, name: str, parameters: Dict[str, Any], scan: Any = None):
                self.name = name
                self.parameters = parameters
                self.scan = scan

        config = PluginConfig(
            type="typed-demo",
            parameters={"custom": {"value": 42}},
        )
        try:
            plugin = create_plugin(config)
        except Exception as exc:  # noqa BLE001 - current registry still expects a dictionary
            pytest.fail(f"create_plugin rejected typed PluginConfig: {exc}")

        assert isinstance(plugin, DemoPlugin)
        assert plugin.name == "typed-demo"
        assert plugin.parameters == {"custom": {"value": 42}}
    finally:
        PLUGIN_REGISTRY.clear()
        PLUGIN_REGISTRY.update(saved)


def test_create_plugin_rejects_raw_mapping_input() -> None:
    from kiwi_scan.plugin.registry import create_plugin

    with pytest.raises(TypeError, match="PluginConfig"):
        create_plugin({
            "type": "legacy-demo",
            "name": "legacy-name",
            "parameters": {"nested": {"value": 7}},
        })
