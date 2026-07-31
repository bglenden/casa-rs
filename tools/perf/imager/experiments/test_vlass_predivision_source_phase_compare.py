#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_predivision_source_phase_compare as subject  # noqa: E402


class PredivisionSourcePhaseCompareTests(unittest.TestCase):
    def test_raw_dtype_matches_frozen_sidecar_abi(self) -> None:
        self.assertEqual(subject.RAW_DTYPE.itemsize, 112)
        self.assertEqual(len(subject.RAW_DTYPE.names or ()), 28)

    def test_source1446_phase_produces_live_operand(self) -> None:
        numerator = np.asarray(
            [[np.asarray([969_396_468, 3_196_932_265], dtype=np.uint32).view(np.complex64)[0], 0j]],
            dtype=np.complex64,
        )
        source_trace = {
            "samples": [
                {
                    "phase_re_bits": 1_065_353_216,
                    "phase_im_bits": 2_932_007_418,
                }
            ]
        }

        phased = subject.predivision_phase_pairs(numerator, source_trace)

        self.assertEqual(
            subject.pair_bits(phased, 0, 0),
            subject.SOURCE_1446_LIVE_NUMERATOR,
        )

    def test_live_operand_wide_division_produces_official_result(self) -> None:
        numerator = np.asarray(
            subject.SOURCE_1446_LIVE_NUMERATOR,
            dtype=np.uint32,
        ).view(np.complex64)[0]
        stored_normalizer = np.asarray(
            [1_064_983_698, 3_161_565_358],
            dtype=np.uint32,
        ).view(np.complex64)[0]

        result = subject.wide_divide_one(numerator, stored_normalizer)

        self.assertEqual(
            np.asarray([result], dtype=np.complex64).view(np.uint32).tolist(),
            subject.SOURCE_1446_LIVE_RESULT,
        )

    def test_classification_closes_both_terms(self) -> None:
        self.assertEqual(
            subject.classify(
                instrumentation_valid=True,
                source_1446_exact=True,
                tt0_mismatches=0,
                tt1_mismatches=0,
                current_counts_valid=True,
            ),
            "predivision-phase-closes-all-terms",
        )

    def test_invalid_current_control_fails_closed(self) -> None:
        self.assertEqual(
            subject.classify(
                instrumentation_valid=True,
                source_1446_exact=True,
                tt0_mismatches=0,
                tt1_mismatches=0,
                current_counts_valid=False,
            ),
            "current-order-control-invalid",
        )


if __name__ == "__main__":
    unittest.main()
