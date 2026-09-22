# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import unittest

from kiwi_scan.tools.axis_pid import (
    AxisPidGains,
    AxisPidInputs,
    AxisPidState,
    compute_axis,
)


def make_inputs(**overrides):
    values = {
        "command": 10.0,
        "command_gradient": 3.0,
        "readback": 8.0,
        "target_current": 2.0,
        "current": 1.0,
        "maximum_velocity": 4.0,
        "dt": 2.0,
    }
    values.update(overrides)
    return AxisPidInputs(**values)


class TestComputeAxis(unittest.TestCase):
    def test_calculates_terms_and_returns_next_state(self):
        gains = AxisPidGains(
            kp=0.5,
            ki=0.1,
            kd=0.25,
            kvf=0.2,
            kvff=0.3,
            analog_offset=0.4,
        )
        initial_state = AxisPidState()

        result, next_state = compute_axis(
            make_inputs(),
            gains,
            initial_state,
        )

        self.assertAlmostEqual(result.following_error, 2.0)
        self.assertAlmostEqual(result.proportional_term, 1.0)
        self.assertAlmostEqual(result.integral_term, 0.4)
        self.assertAlmostEqual(result.derivative_term, 0.25)
        self.assertAlmostEqual(result.velocity_term, 0.8)
        self.assertAlmostEqual(result.velocity_feedforward_term, 0.9)
        self.assertAlmostEqual(result.raw_command, 2.95)
        self.assertAlmostEqual(result.command, 1.0)
        self.assertEqual(result.direction_sign, 1)
        self.assertAlmostEqual(result.command_rate, 0.0)
        self.assertAlmostEqual(result.readback_rate, 0.0)

        self.assertEqual(initial_state, AxisPidState())
        self.assertAlmostEqual(next_state.integral, 4.0)
        self.assertAlmostEqual(next_state.previous_error, 2.0)
        self.assertAlmostEqual(next_state.previous_command, 10.0)
        self.assertAlmostEqual(next_state.previous_readback, 8.0)
        self.assertAlmostEqual(next_state.filtered_command, 1.0)
        self.assertAlmostEqual(next_state.rate_limited_command, 1.0)

    def test_state_is_supplied_explicitly_between_cycles(self):
        first_result, first_state = compute_axis(
            make_inputs(),
            AxisPidGains(kp=0.1),
            AxisPidState(),
        )
        second_result, second_state = compute_axis(
            make_inputs(command=11.0, readback=9.0, dt=1.0),
            AxisPidGains(kp=0.1),
            first_state,
        )

        self.assertAlmostEqual(first_result.command_rate, 0.0)
        self.assertAlmostEqual(second_result.command_rate, 1.0)
        self.assertAlmostEqual(second_result.readback_rate, 1.0)
        self.assertAlmostEqual(second_state.integral, 6.0)

    def test_applies_deadband_without_accumulating_error(self):
        result, next_state = compute_axis(
            make_inputs(command=1.05, readback=1.0, dt=1.0),
            AxisPidGains(kp=1.0, ki=1.0, deadband=0.1),
            AxisPidState(integral=0.5, previous_error=0.0),
        )

        self.assertEqual(result.following_error, 0.0)
        self.assertEqual(result.proportional_term, 0.0)
        self.assertAlmostEqual(result.integral_term, 0.5)
        self.assertAlmostEqual(next_state.integral, 0.5)

    def test_clamps_integral_anti_windup_in_both_directions(self):
        gains = AxisPidGains(ki=1.0, integral_limit=1.0)

        positive, positive_state = compute_axis(
            make_inputs(command=2.0, readback=0.0, dt=1.0),
            gains,
            AxisPidState(integral=0.5),
        )
        negative, negative_state = compute_axis(
            make_inputs(command=-2.0, readback=0.0, dt=1.0),
            gains,
            AxisPidState(integral=-0.5),
        )

        self.assertEqual(positive.integral_term, 1.0)
        self.assertEqual(positive_state.integral, 1.0)
        self.assertEqual(negative.integral_term, -1.0)
        self.assertEqual(negative_state.integral, -1.0)

    def test_applies_filter_before_rate_limit(self):
        result, next_state = compute_axis(
            make_inputs(command=1.0, readback=0.0, dt=1.0),
            AxisPidGains(
                kp=2.0,
                filter_alpha=0.5,
                maximum_delta=0.1,
            ),
            AxisPidState(
                filtered_command=0.2,
                rate_limited_command=0.2,
            ),
        )

        self.assertAlmostEqual(result.raw_command, 2.0)
        self.assertAlmostEqual(next_state.filtered_command, 0.6)
        self.assertAlmostEqual(result.command, 0.3)
        self.assertAlmostEqual(next_state.rate_limited_command, 0.3)

    def test_zero_previous_positions_are_valid_history(self):
        result, _ = compute_axis(
            make_inputs(command=1.0, readback=2.0, dt=1.0),
            AxisPidGains(),
            AxisPidState(
                previous_command=0.0,
                previous_readback=0.0,
            ),
        )

        self.assertAlmostEqual(result.command_rate, 1.0)
        self.assertAlmostEqual(result.readback_rate, 2.0)

    def test_direction_sign_changes_raw_command_direction(self):
        positive, _ = compute_axis(
            make_inputs(target_current=2.0, current=1.0),
            AxisPidGains(kp=0.1),
            AxisPidState(),
        )
        negative, _ = compute_axis(
            make_inputs(target_current=1.0, current=2.0),
            AxisPidGains(kp=0.1),
            AxisPidState(),
        )
        zero, _ = compute_axis(
            make_inputs(target_current=1.0, current=1.0),
            AxisPidGains(kp=0.1),
            AxisPidState(),
        )

        self.assertGreater(positive.raw_command, 0.0)
        self.assertLess(negative.raw_command, 0.0)
        self.assertEqual(zero.raw_command, 0.0)
        self.assertEqual(positive.direction_sign, 1)
        self.assertEqual(negative.direction_sign, -1)
        self.assertEqual(zero.direction_sign, 0)

    def test_rejects_non_positive_sample_interval(self):
        for dt in (0.0, -1.0):
            with self.subTest(dt=dt), self.assertRaisesRegex(ValueError, "dt"):
                    compute_axis(
                        make_inputs(dt=dt),
                        AxisPidGains(),
                        AxisPidState(),
                    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
