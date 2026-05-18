import math
import unittest

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import SubscriptionConfig
from kiwi_scan.scan.stats_collector import StatsCollector


class TestStatsCollector(unittest.TestCase):
    def setUp(self):
        self.energy = SubscriptionConfig(
            actuator="energy",
            source="rbv",
            name="mono_energy",
            role="sync",
        )
        self.beta = SubscriptionConfig(
            pv="XXX:Beta",
            name="beta",
            role="sync",
        )
        self.status = SubscriptionConfig(
            pv="XXX:Status",
            name="status",
            role="status",
        )
        self.collector = StatsCollector([self.energy, self.beta, self.status])

    def _mapping(self):
        return dict(zip(self.collector.get_headers(False), self.collector.get_values()))

    def test_headers_use_actuator_or_subscription_name_prefix(self):
        self.assertEqual(
            self.collector.get_headers(False),
            [
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
            ],
        )

    def test_collects_independent_stats_for_multiple_sync_subscriptions(self):
        self.collector.reset_window()
        for value in (10.0, 12.0, 14.0):
            self.collector.update(PvEvent("ENERGY:RBV", value), self.energy, collect=True)
        for value in (1.0, 3.0):
            self.collector.update(PvEvent("XXX:Beta", value), self.beta, collect=True)

        got = self._mapping()

        self.assertAlmostEqual(got["energyMean"], 12.0)
        self.assertAlmostEqual(got["energyStd"], 2.0)
        self.assertAlmostEqual(got["energyMin"], 10.0)
        self.assertAlmostEqual(got["energyMax"], 14.0)
        self.assertEqual(got["energyNSamples"], 3)

        self.assertAlmostEqual(got["betaMean"], 2.0)
        self.assertAlmostEqual(got["betaStd"], math.sqrt(2.0))
        self.assertAlmostEqual(got["betaMin"], 1.0)
        self.assertAlmostEqual(got["betaMax"], 3.0)
        self.assertEqual(got["betaNSamples"], 2)

    def test_idle_update_is_zero_sample_snapshot_and_reset_preserves_last_value(self):
        self.collector.update(PvEvent("ENERGY:RBV", 7.5), self.energy, collect=False)
        got = self._mapping()
        self.assertAlmostEqual(got["energyMean"], 7.5)
        self.assertAlmostEqual(got["energyStd"], 0.0)
        self.assertAlmostEqual(got["energyMin"], 7.5)
        self.assertAlmostEqual(got["energyMax"], 7.5)
        self.assertEqual(got["energyNSamples"], 0)

        self.collector.reset_window()
        got = self._mapping()
        self.assertAlmostEqual(got["energyMean"], 7.5)
        self.assertEqual(got["energyNSamples"], 0)

        self.collector.update(PvEvent("ENERGY:RBV", 8.5), self.energy, collect=True)
        got = self._mapping()
        self.assertAlmostEqual(got["energyMean"], 8.5)
        self.assertEqual(got["energyNSamples"], 1)

    def test_ignores_non_numeric_and_non_sync_events(self):
        self.collector.update(PvEvent("ENERGY:RBV", "not-a-number"), self.energy, collect=True)
        self.collector.update(PvEvent("XXX:Status", 42), self.status, collect=True)

        got = self._mapping()
        self.assertEqual(got["energyNSamples"], 0)
        self.assertEqual(got["betaNSamples"], 0)

    def test_update_last_point_adds_provider_columns(self):
        self.collector.update(PvEvent("ENERGY:RBV", 1.0), self.energy, collect=True)
        last_point = {}
        self.collector.update_last_point(last_point)

        self.assertIn("energyMean", last_point)
        self.assertEqual(last_point["energyNSamples"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
