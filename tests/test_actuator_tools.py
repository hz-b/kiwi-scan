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
