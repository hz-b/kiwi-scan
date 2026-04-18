import unittest
from kiwi_scan import stats

class TestRunningMean(unittest.TestCase):
    def test_matches_river_mean_example(self):
        values = [-5, -3, -1, 1, 3, 5]
        expected = [-5.0, -4.0, -3.0, -2.0, -1.0, 0.0]

        mean = stats.Mean()
        actual = []
        for value in values:
            mean.update(value)
            actual.append(mean.get())

        self.assertEqual(actual, expected)
        self.assertEqual(mean.n, len(values))

    def test_revert_restores_previous_state(self):
        mean = stats.Mean()
        mean.update(2.0).update(4.0).update(6.0)
        mean.revert(6.0)
        self.assertAlmostEqual(mean.get(), 3.0)
        self.assertAlmostEqual(mean.n, 2.0)


class TestRunningVariance(unittest.TestCase):
    def test_matches_river_var_example(self):
        values = [3, 5, 4, 7, 10, 12]
        expected = [0.0, 2.0, 1.0, 2.9166666666666666, 7.7, 12.566666666666666]

        var = stats.Var()
        actual = []
        for value in values:
            var.update(value)
            actual.append(var.get())

        for got, want in zip(actual, expected):
            self.assertAlmostEqual(got, want)

    def test_population_variance_with_ddof_zero(self):
        values = [1.0, 2.0, 3.0, 4.0]
        var = stats.Var(ddof=0)
        for value in values:
            var.update(value)
        self.assertAlmostEqual(var.get(), 1.25)

    def test_zero_before_enough_samples(self):
        var = stats.Var()
        self.assertEqual(var.get(), 0.0)
        var.update(5.0)
        self.assertEqual(var.get(), 0.0)

    def test_revert_restores_previous_state(self):
        var = stats.Var()
        var.update(3.0).update(5.0).update(4.0)
        self.assertAlmostEqual(var.get(), 1.0)
        var.revert(4.0)
        self.assertAlmostEqual(var.get(), 2.0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
