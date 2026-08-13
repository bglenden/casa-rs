#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

import numpy as np


MODULE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(MODULE_DIR))
SPEC = importlib.util.spec_from_file_location(
    "vlass_taylor_power_graph_compare",
    MODULE_DIR / "vlass_taylor_power_graph_compare.py",
)
assert SPEC is not None and SPEC.loader is not None
subject = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(subject)


class TaylorPowerGraphCompareTest(unittest.TestCase):
    def test_float_scaling_and_addition_are_component_separate(self) -> None:
        tt0 = np.asarray(
            [[complex(np.float32(1.0), np.float32(-2.0))] * 2],
            dtype=np.complex64,
        )
        tt1 = np.asarray(
            [[complex(np.float32(0.1), np.float32(0.2))] * 2],
            dtype=np.complex64,
        )
        power = np.asarray([np.float32(-0.75)], dtype=np.float32)
        scaled = subject.separately_scale_pairs(tt1, power)
        combined = subject.separately_add_pairs(tt0, scaled)
        for role in range(2):
            self.assertEqual(
                int(np.float32(scaled[0, role].real).view(np.uint32)),
                int(np.float32(np.float32(0.1) * power[0]).view(np.uint32)),
            )
            self.assertEqual(
                int(np.float32(combined[0, role].imag).view(np.uint32)),
                int(
                    np.float32(
                        np.float32(-2.0)
                        + np.float32(np.float32(0.2) * power[0])
                    ).view(np.uint32)
                ),
            )

    def test_classification_requires_all_three_exact_boundaries(self) -> None:
        exact = {
            "power_mismatch_count": 0,
            "scaled_tt1_mismatch_count": 0,
            "combined_mismatch_count": 0,
        }
        partial = {
            "power_mismatch_count": 0,
            "scaled_tt1_mismatch_count": 1,
            "combined_mismatch_count": 1,
        }
        nonmatch = {
            "power_mismatch_count": 1,
            "scaled_tt1_mismatch_count": 1,
            "combined_mismatch_count": 1,
        }
        graphs = {
            "source": {**exact, "power_sha256": "source"},
            "casacore": {**exact, "power_sha256": "source"},
            "standard": {**nonmatch, "power_sha256": "standard"},
            "identity": {**nonmatch, "power_sha256": "identity"},
            "late_frequency_cast": {**nonmatch, "power_sha256": "late"},
        }
        equality = {
            "source==casacore": True,
            "source==standard": False,
        }
        helper = {"source_expression_is_float": True}
        self.assertEqual(
            subject.classify(graphs, equality, helper),
            "source-casacore-pow-closes-all",
        )
        graphs["source"] = {**partial, "power_sha256": "source"}
        graphs["casacore"] = {**partial, "power_sha256": "source"}
        self.assertEqual(
            subject.classify(graphs, equality, helper),
            "power-exact-downstream-different",
        )

    def test_signed_ulp_distance_is_monotonic(self) -> None:
        negative = np.asarray([-1.0], dtype=np.float32)
        toward_zero = np.nextafter(
            negative,
            np.asarray([0.0], dtype=np.float32),
            dtype=np.float32,
        )
        self.assertEqual(subject.maximum_ulp_distance(negative, toward_zero), 1)


if __name__ == "__main__":
    unittest.main()
