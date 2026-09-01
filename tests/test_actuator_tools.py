# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import os
import tempfile
import textwrap
import unittest

from kiwi_scan.actuator.tools import load_actuators
from kiwi_scan.actuator_concrete.single_simulation import SimulatedActuator
from kiwi_scan.datamodels import MonitorSpec


class TestLoadActuators(unittest.TestCase):
    def test_loads_actuators_with_replacements_and_keeps_config(self):
        yaml_text = textwrap.dedent(
            """
            actuators:
              theta:
                type: sim
                pv: "SIM:THETA"
                rb_pv: "${IOC}:mono:ThetaRBV"
                status_pv: "${IOC}:mono:ThetaStatus"
            """
        ).strip()

        with tempfile.TemporaryDirectory() as td:
            config_file = os.path.join(td, "scan.yaml")
            with open(config_file, "w", encoding="utf-8") as stream:
                stream.write(yaml_text)

            actuators = load_actuators(config_file, {"IOC": "TESTIOC"})

        actuator = actuators["theta"]
        self.assertIsInstance(actuator, SimulatedActuator)
        self.assertEqual(actuator.config.rb_pv, "TESTIOC:mono:ThetaRBV")
        self.assertEqual(actuator.config.status_pv, "TESTIOC:mono:ThetaStatus")

    def test_monitor_spec_resolves_against_loaded_actuator_config(self):
        yaml_text = textwrap.dedent(
            """
            actuators:
              theta:
                type: sim
                pv: "SIM:THETA"
                status_pv: "TESTIOC:mono:ThetaStatus"
            """
        ).strip()

        with tempfile.TemporaryDirectory() as td:
            config_file = os.path.join(td, "scan.yaml")
            with open(config_file, "w", encoding="utf-8") as stream:
                stream.write(yaml_text)

            actuator = load_actuators(config_file)["theta"]

        monitor = MonitorSpec.from_arg("theta:status")
        self.assertEqual(
            monitor.resolve_pv(actuator.config),
            "TESTIOC:mono:ThetaStatus",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)


class _FakeMonitorActuator:
    def __init__(self):
        from kiwi_scan.datamodels import ActuatorConfig

        self.config = ActuatorConfig(
            pv="SIM:THETA",
            status_pv="TESTIOC:mono:ThetaStatus",
        )
        self.added = []
        self.removed = []

    def supports_monitors(self):
        return True

    def add_monitor(self, pvname, user_callback=None, **_kwargs):
        self.added.append(pvname)
        if user_callback is not None:
            from kiwi_scan.actuator.single import PvEvent

            user_callback(PvEvent(pvname=pvname, value=7, source="test"))
        return object()

    def remove_monitor(self, pvname):
        self.removed.append(pvname)


class TestRunMonitors(unittest.TestCase):
    def test_run_monitors_resolves_config_counts_event_and_cleans_up(self):
        import contextlib
        import io

        import kiwi_scan.actuator.tools as actuator_tools

        self.assertTrue(
            hasattr(actuator_tools, "run_monitors"),
            "actuator.tools must expose run_monitors()",
        )

        actuator = _FakeMonitorActuator()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            seen, dropped = actuator_tools.run_monitors(
                {"theta": actuator},
                ["theta:status"],
                count=1,
            )

        self.assertEqual((seen, dropped), (1, 0))
        self.assertEqual(actuator.added, ["TESTIOC:mono:ThetaStatus"])
        self.assertEqual(actuator.removed, ["TESTIOC:mono:ThetaStatus"])
        self.assertIn("theta:status", output.getvalue())
        self.assertIn("value=7", output.getvalue())

    def test_run_monitors_waits_for_external_completion_condition(self):
        import kiwi_scan.actuator.tools as actuator_tools

        self.assertTrue(
            hasattr(actuator_tools, "run_monitors"),
            "actuator.tools must expose run_monitors()",
        )

        actuator = _FakeMonitorActuator()
        checks = iter((False, True))

        seen, dropped = actuator_tools.run_monitors(
            {"theta": actuator},
            ["theta:status"],
            count=1,
            completion_check=lambda: next(checks),
        )

        self.assertEqual((seen, dropped), (1, 0))
