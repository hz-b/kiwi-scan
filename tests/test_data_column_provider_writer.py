import os
import sys
import tempfile
import types
import unittest

# The writer methods do not need a live pyepics installation.  Provide a tiny
# import-time stub so this unit test can run on non-EPICS CI hosts too.
if "epics" not in sys.modules:
    epics_stub = types.ModuleType("epics")

    class _CA:
        @staticmethod
        def use_initial_context():
            return None

        @staticmethod
        def poll():
            return None

    class _PV:
        def __init__(self, *args, **kwargs):
            self.connected = True
            self.timestamp = None
            self.severity = None
            self.status = None

        def wait_for_connection(self, timeout=None):
            return True

    epics_stub.ca = _CA()
    epics_stub.PV = _PV
    sys.modules["epics"] = epics_stub

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import SubscriptionConfig
from kiwi_scan.scan._point_frame import _DetectorLayout
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.output_manager import OutputManager
from kiwi_scan.scan.performance_tracker import PerformanceTracker
from kiwi_scan.scan.stats_collector import StatsCollector


class DummyScan(BaseScan):
    def __init__(self):
        # The real BaseScan constructor always establishes these attributes.
        # This lightweight writer test intentionally skips hardware setup.
        self.detector_pvs = []
        self.plugins = []
        self.include_timestamps = False
        self.timestamp_output_format = "iso8601"
        self.performance = PerformanceTracker(enabled=False)
        self.output_manager = OutputManager(
            data_dir=".",
            requested_output_file="unused.txt",
            data_writing_enabled=False,
        )
        self._initialize_point_pipeline()

    def execute(self):
        pass


class _DetectorPV:
    def __init__(self, pvname):
        self.pvname = pvname


class TestDataColumnProviderWriter(unittest.TestCase):
    def _make_scan(self, output_file, collector):
        scan = DummyScan()
        scan.output_manager = OutputManager(
            data_dir=os.path.dirname(output_file),
            requested_output_file=os.path.basename(output_file),
            data_writing_enabled=True,
            output_timestamp="20260901120000",
        )
        scan.output_manager.set_header_factory(
            scan._point_pipeline.build_output_headers
        )
        scan.output_file = output_file
        scan.detector_pvs = [_DetectorPV("DET:COUNTS")]
        scan.detector_pvs_monitor = True
        scan.plugins = []
        scan.include_timestamps = False
        scan.timestamp_output_format = "iso8601"
        scan._point_pipeline.set_detector_layout(
            _DetectorLayout.from_headers(
                pv.pvname for pv in scan.detector_pvs
            )
        )
        scan._point_pipeline.replace_last_point({})
        scan.add_column_provider(collector)
        return scan

    def test_header_row_and_get_value_include_provider_columns(self):
        energy = SubscriptionConfig(
            actuator="energy",
            source="rbv",
            name="mono_energy",
            role="stat",
        )
        beta = SubscriptionConfig(
            pv="XXX:Beta",
            name="beta",
            role="stat",
        )
        collector = StatsCollector([energy, beta])
        collector.reset_window()
        collector.update(PvEvent("ENERGY:RBV", 10.0), energy, collect=True)
        collector.update(PvEvent("ENERGY:RBV", 14.0), energy, collect=True)
        collector.update(PvEvent("XXX:Beta", 5.0), beta, collect=True)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "scan.txt")
            scan = self._make_scan(path, collector)
            scan.save_to_file(
                100.0,
                [{"value": 123.0, "timestamp": 1.0, "pvname": "DET:COUNTS"}],
                include_timestamps=False,
            )

            with open(path, "r", encoding="utf-8") as handle:
                lines = [line.rstrip("\n") for line in handle]

        header = lines[0].split("\t")
        row = lines[1].split("\t")

        self.assertEqual(
            header[:12],
            [
                "Position",
                "energyMean",
                "energyStd",
                "energyMin",
                "energyMax",
                "energyNSamples",
                "betaMean",
                "betaStd",
                "betaMin",
                "betaMax",
                "betaNSamples",
                "TS-ISO8601",
            ],
        )
        self.assertEqual(header[12], "DET:COUNTS")

        self.assertAlmostEqual(float(row[0]), 100.0)
        self.assertAlmostEqual(float(row[1]), 12.0)
        self.assertEqual(int(float(row[5])), 2)
        self.assertAlmostEqual(float(row[6]), 5.0)
        self.assertEqual(int(float(row[10])), 1)
        self.assertAlmostEqual(float(row[12]), 123.0)

        self.assertAlmostEqual(scan.get_value("energyMean"), 12.0)
        self.assertEqual(scan.get_value("energyNSamples"), 2)
        self.assertAlmostEqual(scan.get_value("betaMean"), 5.0)
        self.assertAlmostEqual(scan.get_value("DET:COUNTS"), 123.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
