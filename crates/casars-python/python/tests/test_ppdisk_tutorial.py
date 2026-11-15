# SPDX-License-Identifier: LGPL-3.0-or-later
"""Negative controls for the tutorial's scientific metadata comparison."""
import copy
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tools" / "simulation"))
from compare_ppdisk_tutorial import compare_coordinates


class CoordinateComparison(unittest.TestCase):
    def test_frequency_cannot_hide_angular_reference_or_increment_errors(self):
        expected = dict(reference=[2.1, -0.4, 1., 672e9],
                        increment=[-1e-8, 1e-8, 1., 32e9],
                        reference_pixel=[96., 96., 0., 0.],
                        frames=dict(direction=['J2000'], spectral=['LSRK']),
                        projection=dict(type='SIN', parameters=[0., 0.]))
        compare_coordinates(expected, expected)
        for field, axis, replacement in [('reference', 0, 2.2),
                                         ('increment', 0, -1.01e-8),
                                         ('reference_pixel', 0, 97.)]:
            actual = copy.deepcopy(expected)
            actual[field][axis] = replacement
            with self.subTest(field=field), self.assertRaises(AssertionError):
                compare_coordinates(actual, expected)
        actual = copy.deepcopy(expected)
        actual['frames']['spectral'] = ['TOPO']
        with self.assertRaises(AssertionError):
            compare_coordinates(actual, expected)


if __name__ == '__main__':
    unittest.main()
