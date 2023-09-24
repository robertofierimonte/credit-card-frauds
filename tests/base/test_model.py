import unittest

import numpy as np
import xmlrunner

from src.base.model import calculate_precision_top_k


class TestPrecisionTopK(unittest.TestCase):

    y1 = np.array([0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0])
    preds1 = np.array([
        0.001, 0.2, 0.33, 0.00001, 0.002, 0.1, 0.1, 0, 0.1, 0.2, 0.1, 0.9, 0.4,
        0.0001, 0, 0, 0.3, 0.66, 0.234, 0.1212, 0.67, 0.599, 0.4, 0., 0., 0.12
    ])

    y2 = np.array([0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0])
    preds2 = np.array([
        0.99, 0., 0, 0.4, 0.1, 0.3, 0.23, 0.1111, 0.5, 0.44, 0.7, 0.21, 0.22,
        0.56, 0.56, 0.97, 0, 0, 0, 0, 0, 0, 0.1, 0.23, 0.6, 0.0001,
    ])

    def test_precision_top_k_results(self):
        precision1 = calculate_precision_top_k(self.y1, self.preds1, k=10)
        precision2 = calculate_precision_top_k(self.y2, self.preds2, k=10)

        self.assertAlmostEqual(precision1, 0.2)
        self.assertAlmostEqual(precision2, 0.2)

    def test_precision_top_k_different_k(self):
        precision1 = calculate_precision_top_k(self.y1, self.preds1, k=5)
        precision2 = calculate_precision_top_k(self.y2, self.preds2, k=8)

        self.assertAlmostEqual(precision1, 0.4)
        self.assertAlmostEqual(precision2, 0.25)


if __name__ == "__main__":
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-tests.xml"),
        failfast=False,
    )
