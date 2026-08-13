#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_source_phase_placement_compare as subject  # noqa: E402


class SourcePhasePlacementCompareTests(unittest.TestCase):
    def test_phase_pairs_uses_separately_rounded_complex32_graph(self) -> None:
        values = np.array([[1.25 - 0.5j, -2.0 + 0.75j]], dtype=np.complex64)
        phase = np.complex64(0.75 + 0.25j)
        trace = {
            "samples": [
                {
                    "phase_re_bits": int(
                        np.asarray([phase.real], dtype=np.float32).view(np.uint32)[0]
                    ),
                    "phase_im_bits": int(
                        np.asarray([phase.imag], dtype=np.float32).view(np.uint32)[0]
                    ),
                }
            ]
        }

        actual = subject.phase_pairs(values, trace)

        expected = np.array([[1.0625 - 0.0625j, -1.6875 + 0.0625j]])
        np.testing.assert_array_equal(actual, expected.astype(np.complex64))

    def test_classifies_casa_power_as_remaining_owner(self) -> None:
        classification = subject.classify(
            instrumentation_valid=True,
            raw_term_mismatches=0,
            aligned_term_mismatches=0,
            casa_power_combined_exact=True,
            casa_power_residual_exact=True,
            rust_power_combined_exact=False,
            rust_power_residual_exact=False,
        )

        self.assertEqual(
            classification,
            "terms-exact-casa-power-closes-combined",
        )

    def test_raw_difference_stops_before_phase_conclusion(self) -> None:
        classification = subject.classify(
            instrumentation_valid=True,
            raw_term_mismatches=1,
            aligned_term_mismatches=1,
            casa_power_combined_exact=False,
            casa_power_residual_exact=False,
            rust_power_combined_exact=False,
            rust_power_residual_exact=False,
        )

        self.assertEqual(classification, "unphased-raw-terms-still-differ")

    def test_invalid_identity_fails_closed(self) -> None:
        classification = subject.classify(
            instrumentation_valid=False,
            raw_term_mismatches=0,
            aligned_term_mismatches=0,
            casa_power_combined_exact=True,
            casa_power_residual_exact=True,
            rust_power_combined_exact=True,
            rust_power_residual_exact=True,
        )

        self.assertEqual(classification, "invalid-instrumentation")


if __name__ == "__main__":
    unittest.main()
