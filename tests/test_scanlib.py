import sys
import tempfile
import venv
from pathlib import Path
import subprocess
import time
import math
import logging
import pdb
import unittest
from unittest import TestCase
from unittest.mock import patch
from kiwi_scan.scan_concrete.linear import LinearScan
from kiwi_scan.scan_concrete.approach import ApproachMove
from kiwi_scan.scan_concrete.cm import CMScan
from kiwi_scan.actuator_concrete.single_epics import EpicsActuator
from kiwi_scan.actuator_concrete.undulator import UndulatorViaCAN

from kiwi_scan.datamodels import ActuatorConfig, ScanDimension, ScanConfig, JogConfig

class TestKiwiScanInstall(unittest.TestCase):
    def test_clean_install_in_temp_venv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            venv_dir = tmp_path / "venv"

            venv.EnvBuilder(with_pip=True).create(venv_dir)

            python = venv_dir / "bin" / "python"
            pip = venv_dir / "bin" / "pip"

            subprocess.check_call([str(pip), "install", "."])

            result = subprocess.run(
                [str(python), "-c", "import kiwi_scan; print('OK')"],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("OK", result.stdout)


class TestScanRunnerCLI(unittest.TestCase):
    def test_scan_runner_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            # Minimal config file
            config_file = tmp_path / "minimal.yaml"
            config_file.write_text("""
actuators:
  x:
    type: sim
    pv: TEST:PV

detector_pvs: []
scan_dimensions:
  - actuator: x
    start: 0
    stop: 1
    steps: 2
data_dir: scandata
output_file: scan_results.txt
""")

            cmd = [
                sys.executable,
                "-m", "kiwi_scan.scan_runner",
                "--scan_type", "linear",
                "--config-file", str(config_file),
                "--dim", "actuator=x,start=0,stop=1,steps=2"
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("Scan Type:", result.stdout)

class TestScanDimension(unittest.TestCase):
    def test_from_dict_known_fields(self):
        data = {'actuator': 'x', 'start': 0.0, 'stop': 1.0, 'steps': 5, 'velocity': 0.2}
        dim = ScanDimension.from_dict(data)
        self.assertEqual(dim.actuator, 'x')
        self.assertEqual(dim.start, 0.0)
        self.assertEqual(dim.stop, 1.0)
        self.assertEqual(dim.steps, 5)
        self.assertEqual(dim.velocity, 0.2)

    def test_list_from_dicts_single_and_multiple(self):
        single = {'actuator': 'a', 'start': 1.0, 'stop': 2.0, 'steps': 3, 'velocity': 0.2}
        dims_single = ScanDimension.list_from_dicts(single)
        self.assertIsInstance(dims_single, list)
        self.assertEqual(len(dims_single), 1)
        self.assertEqual(dims_single[0].actuator, 'a')

        multiple = [
            {'actuator': 'a', 'start': 0.0, 'stop': 1.0, 'steps': 2},
            {'actuator': 'b', 'start': 1.0, 'stop': 2.0, 'steps': 3}
        ]
        dims_multi = ScanDimension.list_from_dicts(multiple)
        self.assertEqual(len(dims_multi), 2)
        self.assertEqual([d.actuator for d in dims_multi], ['a', 'b'])

    def test_from_dim_args_parsing(self):
        args = [
            'actuator=x,start=0,stop=10,steps=5,velocity=0',
            'actuator=y,start=1,stop=2,steps=2'
        ]
        dims = ScanDimension.from_dim_args(args)
        self.assertEqual(len(dims), 2)
        self.assertEqual(dims[0].actuator, 'x')
        self.assertEqual(dims[0].start, 0.0)
        self.assertEqual(dims[0].stop, 10.0)
        self.assertEqual(dims[0].steps, 5)

    def test_compute_positions_linear(self):
        dim_single = ScanDimension('a', 0.0, 5.0, 1, 0.0)
        self.assertEqual(dim_single.compute_positions_linear(), [0.0])

        dim_multi = ScanDimension('b', 0.0, 10.0, 3, 0.0)
        self.assertEqual(dim_multi.compute_positions_linear(), [0.0, 5.0, 10.0])

    def test_get_actuators(self):
        dims = [
            ScanDimension('x', 0.0, 1.0, 2, 0.0),
            ScanDimension('y', 1.0, 2.0, 3, 0.0)
        ]
        self.assertEqual(ScanDimension.get_actuators(dims), ['x', 'y'])

class TestScanConfigParsing(unittest.TestCase):

    def test_actuator_config_with_extras_and_defaults(self):
        raw = {
            "pv": "SOME:ACTUATOR",
            "rb_pv": "SOME:RB",
            "queueing_delay": 0.2,
            "extra_field": "should be ignored"
        }
        act = ActuatorConfig.from_dict(raw)
        self.assertEqual(act.pv, "SOME:ACTUATOR")
        self.assertEqual(act.rb_pv, "SOME:RB")
        self.assertEqual(act.queueing_delay, 0.2)
        self.assertEqual(act.dwell_time, 1.0)  # default
        self.assertFalse(hasattr(act, "extra_field"))

    def test_actuator_config_with_jog_config(self):
        raw = {
            "pv": "SOME:ACTUATOR",
            "jog": {
                "velocity_pv": "ACT:VELOCITY",
                "command_pv": "ACT:JOG_CMD",
                "command_pos": 1.0,
                "command_neg": -1.0
            }
        }
        act = ActuatorConfig.from_dict(raw)
        # JogConfig should be constructed
        self.assertIsNotNone(act.jog)
        self.assertIsInstance(act.jog, JogConfig)
        self.assertEqual(act.jog.velocity_pv, "ACT:VELOCITY")
        self.assertEqual(act.jog.command_pv, "ACT:JOG_CMD")
        self.assertEqual(act.jog.command_pos, 1.0)
        self.assertEqual(act.jog.command_neg, -1.0)

    def test_scan_config_full_parsing(self):
        raw = {
            "actuators": {
                "energy": {
                    "pv": "ENERGY:PV"
                },
                "gap": {
                    "pv": "GAP:PV",
                    "extra": "should be ignored"
                }
            },
            "detector_pvs": ["DET1", "DET2"],
            "scan_dimensions": [
                {
                    "actuator": "energy",
                    "start": 100,
                    "stop": 200,
                    "steps": 10,
                    "velocity": 0.0
                }
            ],
            "data_dir": "scandata",
            "output_file": "results.txt",
            "debug": True,
            "unknown_key": "ignore me"
        }

        config = ScanConfig.from_dict(raw)
        self.assertIn("energy", config.actuators)
        self.assertEqual(config.detector_pvs, ["DET1", "DET2"])
        self.assertEqual(config.data_dir, "scandata")
        self.assertEqual(config.output_file, "results.txt")
        self.assertTrue(config.debug)
        self.assertIsNone(config.nested_scans)
        self.assertEqual(config.scan_dimensions[0].actuator, "energy")

    def test_parallel_scan_with_two_actuators(self):
        raw = {
            "actuators": {
                "energy": {
                    "pv": "ENERGY:PV",
                    "rb_pv": "ENERGY:RB"
                },
                "gap": {
                    "pv": "GAP:PV",
                    "rb_pv": "GAP:RB",
                    "dwell_time": 0.5
                }
            },
            "detector_pvs": ["DET1", "DET2"],
            "parallel_scans": [
                {
                    "actuator": "energy",
                    "start": 100.0,
                    "stop": 200.0,
                    "steps": 5,
                    "velocity": 0.0
                },
                {
                    "actuator": "gap",
                    "start": 10.0,
                    "stop": 20.0,
                    "steps": 5,
                    "velocity": 0.0
                }
            ],
            "output_file": "parallel_scan.txt"
        }

        config = ScanConfig.from_dict(raw)

        # Validate both actuators exist
        self.assertIn("energy", config.actuators)
        self.assertIn("gap", config.actuators)

        # Validate actuator settings
        self.assertEqual(config.actuators["gap"].dwell_time, 0.5)
        self.assertEqual(config.actuators["energy"].rb_pv, "ENERGY:RB")

        # Validate detectors
        self.assertEqual(config.detector_pvs, ["DET1", "DET2"])

        # Validate scan dimensions
        self.assertEqual(len(config.parallel_scans), 2)
        energy_scan = config.parallel_scans[0]
        gap_scan = config.parallel_scans[1]

        self.assertEqual(energy_scan.actuator, "energy")
        self.assertEqual(energy_scan.steps, 5)
        self.assertEqual(gap_scan.actuator, "gap")
        self.assertEqual(gap_scan.start, 10.0)

class TestLinearScan(unittest.TestCase):
    def setUp(self):
        # patch out EPICS connections
        patcher1 = patch.object(LinearScan, '_connect_actuators', return_value=None)
        self.addCleanup(patcher1.stop)
        patcher1.start()
        patcher2 = patch.object(LinearScan, '_connect_detectors', return_value=None)
        self.addCleanup(patcher2.stop)
        patcher2.start()
        # Comment out to test unique file creation per scan object
        #patcher3 = patch.object(LinearScan, 'generate_and_create_file', return_value=None)
        #self.addCleanup(patcher3.stop)
        #patcher3.start()

        # one actuator + matching ScanDimension
        dim = ScanDimension(
            actuator="motor1",
            start=100.0,
            stop=200.0,
            steps=11,
            velocity=0.0
        )
        actuator_cfg = ActuatorConfig(pv="PV:ACT1")
        self.config = ScanConfig(
            actuators={"motor1": actuator_cfg},
            detector_pvs=[],
            scan_dimensions=[dim],
            parallel_scans=None,
            nested_scans=None,
            data_dir="scandata",
            output_file="unused.txt",
            include_timestamps=False,
            debug=False
        )

        self.linear = LinearScan(self.config)

    def test_initialization(self):
        self.assertIsInstance(self.linear, LinearScan)
        self.assertTrue(hasattr(self.linear, "positions"))
        # exactly one actuator
        self.assertEqual(set(self.linear.positions.keys()), {"motor1"})

    def test_positions_values(self):
        pts = self.linear.positions["motor1"]
        # step size = (200 - 100) / (11 - 1) = 100/10 = 10
        expected = [100.0 + i * 10.0 for i in range(11)]
        self.assertEqual(len(pts), 11)
        # check each value
        for actual, exp in zip(pts, expected):
            self.assertTrue(
                math.isclose(actual, exp, rel_tol=1e-12),
                f"{actual} != {exp}"
            )

    def test_number_of_actuators(self):
        self.assertEqual(len(self.config.actuators), 1)
        self.assertEqual(len(self.linear.cfg.actuators), 1)

class TestApproachMove(unittest.TestCase):
    def setUp(self):
        patcher1 = patch.object(ApproachMove, '_connect_actuators', return_value=None)
        self.addCleanup(patcher1.stop)
        patcher1.start()

        patcher2 = patch.object(ApproachMove, '_connect_detectors', return_value=None)
        self.addCleanup(patcher2.stop)
        patcher2.start()

        # Comment out to test unique file creation per scan object
        #patcher3 = patch.object(ApproachMove, 'generate_and_create_file', return_value=None)
        #self.addCleanup(patcher3.stop)
        #patcher3.start()

        # --- Build a simple ScanConfig for one actuator ---
        dim = ScanDimension(
            actuator="motor1",
            start=0.0,
            stop=10.0,
            steps=3,
            velocity=0.0
        )
        actuator_cfg = ActuatorConfig(
            pv="PV:ACT1",
            stop_pv=None
        )
        self.config = ScanConfig(
            actuators={"motor1": actuator_cfg},
            detector_pvs=[],
            scan_dimensions=[dim],
            parallel_scans=None,
            nested_scans=None,
            data_dir="scandata",
            output_file="unused.txt",
            include_timestamps=False,
            debug=False
        )

        # Instantiate
        self.approach = ApproachMove(self.config)

    def test_initialization(self):
        # Should be the right type
        self.assertIsInstance(self.approach, ApproachMove)

        # positions dict must exist and be a dict
        self.assertTrue(hasattr(self.approach, "positions"))
        self.assertIsInstance(self.approach.positions, dict)

        # exactly one actuator
        self.assertEqual(len(self.approach.positions), 1)
        self.assertIn("motor1", self.approach.positions)

    def test_positions_values(self):
        # With start=0, stop=10, N=3, k=0.096 → 
        # [ 0.0, 10 * ( (2-1)/(3-1) )**0.096, 10.0 ]
        actual = self.approach.positions["motor1"]
        self.assertEqual(len(actual), 3)

        # endpoints should be exact
        self.assertEqual(actual[0], 0.0)
        self.assertEqual(actual[2], 10.0)

        # compute expected middle value
        expected_mid = 10.0 * ((1 / 2) ** 0.096)
        # allow a small floating‐point tolerance
        self.assertTrue(
            math.isclose(actual[1], expected_mid, rel_tol=1e-9),
            msg=f"middle value {actual[1]} != expected {expected_mid}"
        )

    def test_number_of_actuators(self):
        # Also exercise len(cfg.actuators)
        n = len(self.config.actuators)
        self.assertEqual(n, 1)
        # And that our scan object sees the same
        self.assertEqual(len(self.approach.cfg.actuators), n)        # self.assertEqual(result, expected_value)
        pass

class TestUndulatorViaCAN(unittest.TestCase):
    def test_pack_velocities_basic(self):
        # Positive values in range
        assert UndulatorViaCAN.pack_velocities(100, 200) == ((200 & 0xFFFF) << 16) | (100 & 0xFFFF)

    def test_pack_velocities_negative_gap(self):
        # Negative gap
        assert UndulatorViaCAN.pack_velocities(-100, 200) == ((200 & 0xFFFF) << 16) | ((-100 & 0xFFFF))

    def test_pack_velocities_negative_shift(self):
        # Negative shift
        assert UndulatorViaCAN.pack_velocities(100, -200) == (((-200 & 0xFFFF) << 16) | (100 & 0xFFFF))

    def test_pack_velocities_both_negative(self):
        # Both negative
        assert UndulatorViaCAN.pack_velocities(-100, -200) == (((-200 & 0xFFFF) << 16) | ((-100 & 0xFFFF)))

    def test_pack_velocities_upper_bound(self):
        # Upper bound
        assert UndulatorViaCAN.pack_velocities(40000, 40000) == ((32767 & 0xFFFF) << 16) | (32767 & 0xFFFF)

    def test_pack_velocities_lower_bound(self):
        # Lower bound
        assert UndulatorViaCAN.pack_velocities(-40000, -40000) == ((-32768 & 0xFFFF) << 16) | ((-32768 & 0xFFFF))

    def test_pack_velocities_rounding(self):
        # Values close to boundaries, test rounding
        assert UndulatorViaCAN.pack_velocities(32767.4, -32767.6) == ((-32768 & 0xFFFF) << 16) | (32767 & 0xFFFF)


if __name__ == '__main__':
    unittest.main(verbosity=2)

