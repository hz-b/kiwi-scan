import os
import sys
import types
import tempfile
import textwrap
import unittest
import importlib.util
from pathlib import Path
from unittest.mock import patch

import kiwi_scan.test_support as test_support

# Install a tiny pyepics stub before importing kiwi_scan modules.
if "epics" not in sys.modules:
    sys.modules["epics"] = test_support.make_fake_epics_module()


import kiwi_scan
from kiwi_scan.datamodels import ActuatorConfig, ScanTriggers
from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import PLUGIN_REGISTRY, create_plugin, register_plugin
from kiwi_scan.scan.registry import SCAN_REGISTRY, load_all_scan_types
from kiwi_scan.scan.trigger_manager import TriggerManager
from kiwi_scan.actuator_concrete.single_epics import EpicsActuator


class _DemoPlugin(ScanPlugin):
    def get_headers(self, timestamps: bool):
        return ["demo"]

    def get_values(self, idx, pos):
        return [idx]


FakeTriggerPV = test_support.make_fake_trigger_pv_class()
FakeMonitorPV = test_support.make_fake_monitor_pv_class(start_index=100)


class TestPluginRegistry(unittest.TestCase):
    def setUp(self):
        self._saved = dict(PLUGIN_REGISTRY)
        PLUGIN_REGISTRY.clear()

    def tearDown(self):
        PLUGIN_REGISTRY.clear()
        PLUGIN_REGISTRY.update(self._saved)

    def test_plugin_registration_by_explicit_name(self):
        alias = "explicit_demo_plugin"

        @register_plugin(alias)
        class ExplicitPlugin(_DemoPlugin):
            pass

        plugin = create_plugin(
            {"type": alias, "name": "friendly", "parameters": {"answer": 42}}
        )

        self.assertIn(alias, PLUGIN_REGISTRY)
        self.assertIs(PLUGIN_REGISTRY[alias], ExplicitPlugin)
        self.assertIsInstance(plugin, ExplicitPlugin)
        self.assertEqual(plugin.name, "friendly")
        self.assertEqual(plugin.parameters, {"answer": 42})

    def test_external_plugin_loading_from_env_path(self):
        with tempfile.TemporaryDirectory() as td:
            plugin_file = Path(td) / "ext_plugin.py"
            plugin_file.write_text(
                textwrap.dedent(
                    """
                    from kiwi_scan.plugin.base import ScanPlugin
                    from kiwi_scan.plugin.registry import register_plugin

                    @register_plugin("env_plugin")
                    class EnvPlugin(ScanPlugin):
                        def get_headers(self, timestamps: bool):
                            return ["env"]

                        def get_values(self, idx, pos):
                            return [idx]
                    """
                ).strip()
            )

            with patch.dict(os.environ, {"KIWI_SCAN_PLUGIN_PATH": td}, clear=False):
                kiwi_scan.load_all_plugins(raise_on_error=True)

        self.assertIn("env_plugin", PLUGIN_REGISTRY)
        plugin = create_plugin({"type": "env_plugin", "name": "loaded"})
        self.assertEqual(plugin.name, "loaded")
        self.assertEqual(plugin.get_headers(False), ["env"])


class TestScanRegistry(unittest.TestCase):
    def setUp(self):
        self._saved = dict(SCAN_REGISTRY)
        SCAN_REGISTRY.clear()

    def tearDown(self):
        SCAN_REGISTRY.clear()
        SCAN_REGISTRY.update(self._saved)

    def test_external_scan_loading_from_env_path(self):
        with tempfile.TemporaryDirectory() as td:
            scan_file = Path(td) / "ext_scan.py"
            scan_file.write_text(
                textwrap.dedent(
                    """
                    from kiwi_scan.scan.registry import register_scan

                    @register_scan("env_scan")
                    class EnvScan:
                        pass
                    """
                ).strip()
            )

            with patch.dict(os.environ, {"KIWI_SCAN_SCAN_PATH": td}, clear=False):
                load_all_scan_types(raise_on_error=True)

        self.assertIn("env_scan", SCAN_REGISTRY)
        self.assertEqual(SCAN_REGISTRY["env_scan"].__name__, "EnvScan")


class TestTriggerParsing(unittest.TestCase):
    @patch("kiwi_scan.scan.trigger_manager.EpicsPV", FakeTriggerPV)
    def test_trigger_parsing_keeps_monitor_and_custom_phases(self):
        triggers = ScanTriggers.from_dict(
            {
                "before": [{"pv": "PV:BEFORE", "value": 1}],
                "monitor": [{"pv": "PV:MON", "value": "[1, 2, 3]", "delay": 0.25}],
                "custom": [{"pv": "PV:CUSTOM", "value": 7}],
            }
        )

        self.assertEqual(len(triggers.before), 1)
        self.assertEqual(len(triggers.monitor), 1)
        self.assertTrue(hasattr(triggers, "custom"))
        self.assertEqual(len(triggers.custom), 1)

        manager = TriggerManager.from_config(triggers)

        self.assertIn("monitor", manager.phases)
        self.assertIn("custom", manager.phases)
        self.assertTrue(manager.has_actions("monitor"))
        self.assertTrue(manager.has_actions("custom"))

        monitor_action = manager._actions_by_phase["monitor"][0]
        custom_action = manager._actions_by_phase["custom"][0]

        self.assertEqual(monitor_action.pv.pvname, "PV:MON")
        self.assertEqual(monitor_action.value, [1.0, 2.0, 3.0])
        self.assertAlmostEqual(monitor_action.delay, 0.25)
        self.assertEqual(custom_action.value, 7)


class TestEpicsActuatorMonitorLifecycle(unittest.TestCase):
    @patch("kiwi_scan.actuator_concrete.single_epics.EpicsPV", FakeMonitorPV)
    def test_callback_add_remove_lifecycle(self):
        FakeMonitorPV.next_index = 100
        actuator = EpicsActuator(
            ActuatorConfig(
                pv="SET:PV",
                rb_pv="READ:PV",
                status_pv="STAT:PV",
                queueing_delay=0.0,
            )
        )

        received = []
        handle = actuator.add_monitor("MON:PV", user_callback=received.append)

        self.assertIs(handle, actuator._monitors["MON:PV"])
        self.assertEqual(actuator._epics_cb_indices["MON:PV"], [100])

        handle.trigger(100, value=12.5, timestamp=3.0, severity=1, status=0)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].pvname, "MON:PV")
        self.assertEqual(received[0].value, 12.5)
        self.assertEqual(received[0].timestamp, 3.0)
        self.assertEqual(received[0].severity, 1)
        self.assertEqual(received[0].status, 0)
        self.assertEqual(actuator.get_last_event("MON:PV").value, 12.5)

        actuator.remove_monitor("MON:PV")

        self.assertEqual(handle._pv.removed, [100])
        self.assertTrue(handle._pv.disconnected)
        self.assertNotIn("MON:PV", actuator._monitors)
        self.assertIsNone(actuator.get_last_event("MON:PV"))

        # Even if a stale callback slipped through, it should be ignored after removal.
        handle.trigger(100, value=99.0)
        self.assertEqual(len(received), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
