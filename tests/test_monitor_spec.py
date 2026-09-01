# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Tests for actuator monitor specifications and PV resolution."""

import unittest

from kiwi_scan.datamodels import ActuatorConfig, MonitorSpec


class TestMonitorSpec(unittest.TestCase):
    """Protect the public monitor-specification data model."""

    def test_from_arg_parses_supported_forms(self):
        """Removing or breaking a supported monitor syntax must fail this test."""
        cases = [
            ("energy", "energy", "rbv", None),
            (" energy:status ", "energy", "status", None),
            ("energy:", "energy", "rbv", None),
            ("energy@IOC:ENERGY", "energy", "pv", "IOC:ENERGY"),
        ]

        for argument, expected_name, expected_source, expected_pv in cases:
            with self.subTest(argument=argument):
                spec = MonitorSpec.from_arg(argument)
                self.assertEqual(spec.name, expected_name)
                self.assertEqual(spec.source, expected_source)
                self.assertEqual(spec.pv, expected_pv)

    def test_from_arg_rejects_invalid_forms(self):
        """Malformed monitor arguments must not create unusable models."""
        for argument in ("", "   ", "@IOC:ENERGY", "energy@", ":status"):
            with self.subTest(argument=argument), self.assertRaises(ValueError):
                MonitorSpec.from_arg(argument)

    def test_resolve_pv_uses_direct_override(self):
        """A NAME@PV request must not require a configured actuator PV."""
        spec = MonitorSpec.from_arg("energy@IOC:ENERGY")

        self.assertEqual(
            spec.resolve_pv(ActuatorConfig()),
            "IOC:ENERGY",
        )

    def test_resolve_pv_uses_requested_actuator_source(self):
        """A NAME:SOURCE request must resolve through ActuatorConfig."""
        config = ActuatorConfig(status_pv="IOC:STATUS")
        spec = MonitorSpec.from_arg("energy:status")

        self.assertEqual(spec.resolve_pv(config), "IOC:STATUS")


class TestActuatorConfigPvResolution(unittest.TestCase):
    """Protect reusable actuator-source to PV-name resolution."""

    def test_resolve_pv_selects_each_supported_source(self):
        """Changing a source mapping must fail with its literal expected PV."""
        config = ActuatorConfig(
            pv="IOC:BASE",
            rb_pv="IOC:RBV",
            cmd_pv="IOC:CMD",
            status_pv="IOC:STATUS",
            stop_pv="IOC:STOP",
            get_velocity_pv="IOC:GET-VELOCITY",
            velocity_pv="IOC:VELOCITY",
            cmdvel_pv="IOC:CMD-VELOCITY",
        )
        cases = [
            ("rbv", "IOC:RBV"),
            ("RBV", "IOC:RBV"),
            ("cmd", "IOC:CMD"),
            ("set", "IOC:CMD"),
            ("command", "IOC:CMD"),
            ("status", "IOC:STATUS"),
            ("stop", "IOC:STOP"),
            ("velocity", "IOC:GET-VELOCITY"),
        ]

        for source, expected_pv in cases:
            with self.subTest(source=source):
                self.assertEqual(config.resolve_pv(source), expected_pv)

    def test_resolve_pv_uses_base_pv_as_fallback(self):
        """Removing the rbv/cmd/velocity fallback must fail this test."""
        config = ActuatorConfig(pv="IOC:BASE")

        for source in ("rbv", "cmd", "velocity"):
            with self.subTest(source=source):
                self.assertEqual(config.resolve_pv(source), "IOC:BASE")

    def test_resolve_pv_uses_velocity_fallback_order(self):
        """Velocity resolution must prefer readback, write, then command PVs."""
        cases = [
            (
                ActuatorConfig(
                    get_velocity_pv="IOC:GET",
                    velocity_pv="IOC:VELOCITY",
                    cmdvel_pv="IOC:CMD",
                    pv="IOC:BASE",
                ),
                "IOC:GET",
            ),
            (
                ActuatorConfig(
                    velocity_pv="IOC:VELOCITY",
                    cmdvel_pv="IOC:CMD",
                    pv="IOC:BASE",
                ),
                "IOC:VELOCITY",
            ),
            (
                ActuatorConfig(
                    cmdvel_pv="IOC:CMD",
                    pv="IOC:BASE",
                ),
                "IOC:CMD",
            ),
        ]

        for config, expected_pv in cases:
            with self.subTest(expected_pv=expected_pv):
                self.assertEqual(config.resolve_pv("velocity"), expected_pv)

    def test_resolve_pv_rejects_missing_source_pv(self):
        """An unresolved Optional PV must never escape as None."""
        config = ActuatorConfig()

        for source in ("rbv", "cmd", "status", "stop", "velocity"):
            with self.subTest(source=source), self.assertRaisesRegex(
                ValueError,
                "has no PV configured",
            ):
                config.resolve_pv(source)

    def test_resolve_pv_rejects_unsupported_source(self):
        """Unknown source names must not silently select the base PV."""
        config = ActuatorConfig(pv="IOC:BASE")

        with self.assertRaisesRegex(ValueError, "Unsupported source"):
            config.resolve_pv("temperature")


if __name__ == "__main__":
    unittest.main()
