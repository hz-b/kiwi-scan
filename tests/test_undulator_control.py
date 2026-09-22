import unittest
from dataclasses import replace
from unittest.mock import patch

import kiwi_scan.tools.axis_pid as pid
from kiwi_scan.tools.undulator_control import UndulatorControlCore


class TestUndulatorControlCore(unittest.TestCase):
    def test_results_match_original_math_over_multiple_cycles(self):
        core = UndulatorControlCore(initial_dt=0.2)
        states = {axis: pid.AxisPidState() for axis in core.enabled_axes}
        gains = {
            axis: pid.AxisPidGains(
                kp=0.3, ki=0.2, kd=0.01, kvf=0.03, kvff=0.04,
                analog_offset=0.01, integral_limit=0.15,
                deadband=0.01, filter_alpha=0.4, maximum_delta=0.02,
            ) for axis in core.enabled_axes
        }
        previous = None
        for i, now in enumerate((10.0, 10.1, 10.1, 10.05, 11.0, 12.0)):
            inputs = {
                axis: pid.AxisPidInputs(
                    command=(-1) ** i * (0.2 + i), command_gradient=0.5,
                    readback=0.1, target_current=(-1) ** i, current=0.0,
                    maximum_velocity=0.7, dt=999,
                ) for axis in core.enabled_axes
            }
            dt = 0.2 if previous is None else max(now - previous, 1e-6)
            expected = {}
            for axis in core.enabled_axes:
                expected[axis], states[axis] = pid.compute_axis(
                    replace(inputs[axis], dt=dt), gains[axis], states[axis]
                )
            self.assertEqual(core.calculate(inputs, gains, now), expected)
            self.assertEqual(core.states, states)
            previous = now

    def test_gap_only_never_evaluates_shift(self):
        core = UndulatorControlCore(["gap"])
        inputs = {"gap": pid.AxisPidInputs(1, 0, 0, 1, 0, 0, 1), "shift": object()}
        with patch(
            "kiwi_scan.tools.undulator_control.compute_axis", wraps=pid.compute_axis
        ) as compute:
            results = core.calculate(inputs, {"gap": pid.AxisPidGains(kp=0.5)}, 1)
        self.assertEqual(compute.call_count, 1)
        self.assertEqual(list(results), ["gap"])
        self.assertEqual(list(core.states), ["gap"])

    def test_missing_active_inputs_do_not_advance_state(self):
        core = UndulatorControlCore()
        original = core.states
        with self.assertRaises(ValueError):
            core.calculate({}, {}, 5)
        self.assertEqual(core.states, original)
        self.assertEqual(core.sample_interval(6), 1)

    def test_failed_second_axis_does_not_partially_commit(self):
        core = UndulatorControlCore()
        sampled = pid.AxisPidInputs(1, 0, 0, 1, 0, 0, 1)
        inputs = dict.fromkeys(core.enabled_axes, sampled)
        gains = dict.fromkeys(core.enabled_axes, pid.AxisPidGains(kp=0.5))
        core.calculate(inputs, gains, 5)
        original = core.states
        gains["shift"] = None
        with self.assertRaises(AttributeError):
            core.calculate(inputs, gains, 6)
        self.assertEqual(core.states, original)
        self.assertEqual(core.sample_interval(7), 2)

    def test_reset_restores_first_cycle_timing_and_history(self):
        core = UndulatorControlCore(["gap"], initial_dt=0.2)
        core.calculate({"gap": pid.AxisPidInputs(1, 0, 0, 1, 0, 0, 1)},
                       {"gap": pid.AxisPidGains(ki=0.5)}, 10)
        core.reset()
        self.assertEqual(core.sample_interval(20), 0.2)
        self.assertEqual(core.states["gap"], pid.AxisPidState())

    def test_states_property_cannot_mutate_controller(self):
        core = UndulatorControlCore(["gap"])
        exposed = core.states
        exposed.clear()
        self.assertIn("gap", core.states)

    def test_reject_string_enabled_axes(self):
        with self.assertRaisesRegex(TypeError, "enabled_axes must be a sequence"):
            UndulatorControlCore("gap")

    def test_reject_invalid_axes_and_timing(self):
        for axes in ([], ["shift"], ["gap", "gap"], ["gap", "other"]):
            with self.subTest(axes=axes), self.assertRaises(ValueError):
                UndulatorControlCore(axes)
        for dt in (0, -1, float("nan"), float("inf")):
            with self.subTest(dt=dt), self.assertRaises(ValueError):
                UndulatorControlCore(initial_dt=dt)
        with self.assertRaises(ValueError):
            UndulatorControlCore().sample_interval(float("nan"))
