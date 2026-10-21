# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Exercise group delay overrides through the real EpicsPV put path."""

import unittest
from unittest.mock import Mock, call, patch

from kiwi_scan.actuator.multi import MultiActuator
from kiwi_scan.actuator_concrete.single_epics import EpicsActuator
from kiwi_scan.actuator_concrete.undulator import UndulatorViaCAN, UndulatorViaEPICS
from kiwi_scan.datamodels import ActuatorConfig, JogConfig
from kiwi_scan.epics_wrapper import EpicsPV
from kiwi_scan.test_support import FakePV


class LocalPutPV(FakePV):
    """Use the normal wrapper put implementation without a CA connection."""

    put = EpicsPV.put
    _require_pv = EpicsPV._require_pv

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pv.put = Mock(return_value=1)
        self.nelm = 2


class TestActuatorQueueingDelay(unittest.TestCase):
    def setUp(self):
        for module in (
            "kiwi_scan.actuator_concrete.single_epics",
            "kiwi_scan.actuator_concrete.undulator",
        ):
            patcher = patch(module + ".EpicsPV", LocalPutPV)
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def make_axis(name="GAP", delay=0.01):
        return EpicsActuator(ActuatorConfig(
            pv=name + ":SET",
            rb_pv=name + ":RBV",
            status_pv=name + ":STATUS",
            cmd_pv=name + ":CMD",
            cmdvel_pv=name + ":GRADIENT",
            rel_pv=name + ":REL",
            start_pv=name + ":START",
            stop_pv=name + ":STOP",
            velocity_pv=name + ":VELOCITY",
            queueing_delay=delay,
            jog=JogConfig(
                velocity_pv=name + ":JOG:VELOCITY",
                command_pv=name + ":JOG:START",
                command_pos=1,
            ),
        ))

    @staticmethod
    def group_config(delay=0.01):
        return ActuatorConfig(
            queueing_delay=delay,
            jog=JogConfig(
                velocity_pv="ID:SPEED",
                command_pv="ID:START",
                command_pos=1,
            ),
        )

    def test_configured_delay_controls_put_sleep_and_zero_disables_it(self):
        actuator = self.make_axis(delay=0.025)

        with patch("kiwi_scan.epics_wrapper.time.sleep") as sleep:
            actuator.jog(0.5, sync=False)
            self.assertEqual(sleep.call_args_list, [call(0.025), call(0.025)])
            sleep.reset_mock()

            actuator.set_queueing_delay(0.0)
            actuator.jog(0.75, sync=False)
            sleep.assert_not_called()

        self.assertEqual(actuator.jog_velocity_pv._pv.put.call_count, 2)
        self.assertEqual(actuator.jog_command_pv._pv.put.call_count, 2)
        for channel in (
            actuator.pv, actuator.rel_pv, actuator.cmd_pv,
            actuator.cmdvel_pv, actuator.start_pv, actuator.stop_pv,
            actuator.velocity_pv,
        ):
            self.assertEqual(channel.queueing_delay, 0.0)
        self.assertEqual(actuator.rb_pv.queueing_delay, 0.0)
        self.assertEqual(actuator.status_pv.queueing_delay, 0.0)

    def test_nested_group_override_preserves_children_until_explicit_call(self):
        gap = self.make_axis("GAP", delay=0.02)
        shift = self.make_axis("SHIFT", delay=0.03)
        nested = MultiActuator([shift], ActuatorConfig(queueing_delay=0.04))
        group = MultiActuator([gap, nested], ActuatorConfig(queueing_delay=0.05))

        self.assertEqual((gap.q_delay, shift.q_delay), (0.02, 0.03))
        group.set_queueing_delay(0.0)

        with patch("kiwi_scan.epics_wrapper.time.sleep") as sleep:
            gap.jog(0.25, sync=False)
            shift.jog(0.5, sync=False)
            sleep.assert_not_called()
        self.assertEqual((group.q_delay, nested.q_delay), (0.0, 0.0))

    def test_gap_only_undulator_config_and_override_reach_shared_outputs(self):
        for backend in (UndulatorViaEPICS, UndulatorViaCAN):
            with self.subTest(backend=backend.__name__):
                gap = self.make_axis(delay=0.02)
                undulator = backend(gap, None, self.group_config(delay=0.03))
                self.assertEqual(gap.q_delay, 0.02)

                with patch("kiwi_scan.epics_wrapper.time.sleep") as sleep:
                    undulator.jog([0.5, 0.0], sync=False)
                    self.assertEqual(
                        sleep.call_args_list, [call(0.03), call(0.03)]
                    )
                    sleep.reset_mock()

                    undulator.set_queueing_delay(0.0)
                    undulator.jog([0.75, 0.0], sync=False)
                    sleep.assert_not_called()
                self.assertEqual(gap.q_delay, 0.0)

    def test_invalid_group_delay_does_not_modify_group_or_children(self):
        gap = self.make_axis(delay=0.02)
        group = UndulatorViaEPICS(gap, None, self.group_config(delay=0.03))

        for delay in (-0.1, float("nan"), float("inf"), -float("inf")):
            with self.subTest(delay=delay):
                with self.assertRaisesRegex(ValueError, "finite and non-negative"):
                    group.set_queueing_delay(delay)
                self.assertEqual(group.q_delay, 0.03)
                self.assertEqual(gap.q_delay, 0.02)
                self.assertEqual(group.jog_velocity_pv.queueing_delay, 0.03)

    def test_undulator_reports_failed_optional_start_put(self):
        gap = self.make_axis(delay=0.0)
        undulator = UndulatorViaEPICS(gap, None, self.group_config(delay=0.0))
        undulator.jog_command_pv._pv.put.side_effect = OSError("disconnected")

        with self.assertLogs(level="ERROR"), self.assertRaisesRegex(
            RuntimeError, "jog start command"
        ):
            undulator.jog([0.5, 0.0], sync=False)

    def test_leaf_creation_failure_closes_earlier_outputs_and_feedback_pvs(self):
        created = []
        error = ConnectionError("jog start PV unavailable")

        def create_pv(pvname, **kwargs):
            if pvname == "GAP:JOG:START":
                raise error
            pv = LocalPutPV(pvname, **kwargs)
            pv.close = Mock(side_effect=pv.disconnect)
            created.append(pv)
            return pv

        with patch(
            "kiwi_scan.actuator_concrete.single_epics.EpicsPV",
            side_effect=create_pv,
        ), self.assertRaises(ConnectionError) as captured:
            self.make_axis()

        self.assertIs(captured.exception, error)
        self.assertIn("GAP:RBV", [pv.pvname for pv in created])
        self.assertIn("GAP:STATUS", [pv.pvname for pv in created])
        for pv in created:
            pv.close.assert_called_once_with()
            self.assertTrue(pv.disconnected)

    def test_leaf_validation_failure_disconnects_created_channels(self):
        created = []
        error = ConnectionError("setter became unavailable")

        def create_pv(pvname, **kwargs):
            pv = LocalPutPV(pvname, **kwargs)
            if pvname == "GAP:SET":
                pv.check_pv_exception = error
            created.append(pv)
            return pv

        with patch(
            "kiwi_scan.actuator_concrete.single_epics.EpicsPV",
            side_effect=create_pv,
        ), self.assertRaises(ConnectionError) as captured:
            self.make_axis()

        self.assertIs(captured.exception, error)
        self.assertIn("GAP:JOG:START", [pv.pvname for pv in created])
        self.assertTrue(all(pv.disconnected for pv in created))

    def test_failed_undulator_construction_closes_only_owned_output_pvs(self):
        gap = self.make_axis()
        created = []
        error = ConnectionError("ID start unavailable")

        def create_pv(pvname, **kwargs):
            if pvname == "ID:START":
                raise error
            pv = LocalPutPV(pvname, **kwargs)
            created.append(pv)
            return pv

        with patch(
            "kiwi_scan.actuator_concrete.undulator.EpicsPV",
            side_effect=create_pv,
        ), self.assertRaises(ConnectionError) as captured:
            UndulatorViaEPICS(gap, None, self.group_config())

        self.assertIs(captured.exception, error)
        self.assertEqual([pv.pvname for pv in created], ["ID:SPEED"])
        self.assertTrue(created[0].disconnected)
        self.assertFalse(gap.pv.disconnected)
        self.assertFalse(gap.rb_pv.disconnected)
        self.assertFalse(gap.jog_velocity_pv.disconnected)


if __name__ == "__main__":
    unittest.main()
