#!/usr/bin/env python3
"""Focused tests for the matched CASA/casa-rs residual source comparator."""

from __future__ import annotations

import importlib.util
import struct
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("vlass_aw_residual_sample_compare.py")
SPEC = importlib.util.spec_from_file_location("vlass_aw_residual_sample_compare", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def bits(value: float) -> int:
    return struct.unpack("<I", struct.pack("<f", value))[0]


class ResidualSampleCompareTest(unittest.TestCase):
    def test_boundary_complex_bits_selects_requested_boundary_and_polarization(
        self,
    ) -> None:
        record = {
            "boundaries": {
                "combined_rr_re": {"bits": bits(1.25)},
                "combined_rr_im": {"bits": bits(-0.5)},
                "combined_ll_re": {"bits": bits(2.0)},
                "combined_ll_im": {"bits": bits(3.0)},
            }
        }
        self.assertEqual(
            MODULE.boundary_complex_bits(record, "combined", "rr"),
            [bits(1.25), bits(-0.5)],
        )

    def test_intermediate_sample_cannot_replace_term_prediction(self) -> None:
        raw = [bits(-0.3339190483), bits(0.00157418754)]
        intermediate_residual = [bits(-0.5043013), bits(0.06356374)]
        term_prediction = [bits(0.2155215889), bits(-0.0784112066)]
        sample_prediction = MODULE.f32_subtract(raw, intermediate_residual)
        comparison = MODULE.complex_comparison(term_prediction, sample_prediction)
        self.assertGreater(comparison["relative_difference"], 0.20)

    def test_term_prediction_constructs_authoritative_residual(self) -> None:
        raw = [bits(-0.3339190483), bits(0.00157418754)]
        term_prediction = [bits(0.2155215889), bits(-0.0784112066)]
        residual = MODULE.f32_subtract(raw, term_prediction)
        value = complex(
            MODULE.float_from_bits(residual[0]),
            MODULE.float_from_bits(residual[1]),
        )
        self.assertAlmostEqual(value.real, -0.5494406, places=6)
        self.assertAlmostEqual(value.imag, 0.0799854, places=6)


if __name__ == "__main__":
    unittest.main()
