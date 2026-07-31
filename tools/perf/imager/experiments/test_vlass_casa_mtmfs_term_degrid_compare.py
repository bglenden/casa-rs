#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_casa_mtmfs_term_degrid_compare as subject  # noqa: E402


class CasaMtmfsTermDegridCompareTests(unittest.TestCase):
    def test_binary_layout_matches_interposer(self) -> None:
        self.assertEqual(subject.CASA_DTYPE.itemsize, 104)
        self.assertEqual(subject.CASA_DTYPE.fields["frequency_hz"][1], 24)
        self.assertEqual(subject.CASA_DTYPE.fields["tt0_rr_re"][1], 40)
        self.assertEqual(subject.CASA_DTYPE.fields["combined_ll_im"][1], 100)

    def test_separate_scaling_is_componentwise_f32(self) -> None:
        values = np.array([[2.0 + 4.0j, -3.0 + 5.0j]], dtype=np.complex64)
        powers = np.array([0.25], dtype=np.float32)

        actual = subject.separately_scale_pairs(values, powers)

        np.testing.assert_array_equal(
            actual,
            np.array([[0.5 + 1.0j, -0.75 + 1.25j]], dtype=np.complex64),
        )

    def test_fnv1a64_file_matches_known_vector(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hello"
            path.write_bytes(b"hello")

            self.assertEqual(subject.fnv1a64_file(path), 0xA430D84680AABD0B)

    def test_selected_rows_map_back_to_original_ms_blocks(self) -> None:
        records = np.zeros(12, dtype=subject.CASA_DTYPE)
        records["row_id"] = np.repeat(np.arange(6, dtype=np.uint64), 2)
        records["spw_id"] = np.repeat([2, 2, 2, 4, 4, 4], 2)
        records["channel"] = np.tile([0, 1], 6)
        records["frequency_hz"] = np.tile([10.0, 11.0], 6)
        source_trace = {
            "samples": [
                {
                    "source_ordinal": 0,
                    "row_id": 100,
                    "spw_id": 2,
                    "channel": 1,
                    "frequency_hz": 11.0,
                },
                {
                    "source_ordinal": 1,
                    "row_id": 108,
                    "spw_id": 4,
                    "channel": 0,
                    "frequency_hz": 10.0,
                },
            ]
        }
        casa_trace = {
            "row_id": np.array([100, 101, 102, 106, 107, 108]),
            "spectral_window_id": np.array([2, 2, 2, 4, 4, 4]),
        }

        selected, identity = subject.select_source_records(
            records,
            source_trace,
            casa_trace,
        )

        self.assertEqual(selected["row_id"].tolist(), [0, 5])
        self.assertEqual(identity["rows_per_spw"], 3)
        self.assertEqual(identity["original_row_base"], 94)

    def test_classification_stops_at_tt0(self) -> None:
        zero = np.zeros((1, 2), dtype=np.complex64)
        casars_tt0 = zero.copy()
        casars_tt0[0, 0] = np.complex64(1.0)

        classification, first = subject.classify(
            casars_tt0=casars_tt0,
            casars_tt1=zero,
            casars_scaled=zero,
            casars_literal_combined=zero,
            casa_tt0_raw=zero,
            casa_tt1_raw=zero,
            casa_tt1_scaled_raw=zero,
            casa_combined_raw=zero,
            casa_tt0_rotated=zero,
            casa_tt1_rotated=zero,
            casa_tt1_scaled_rotated=zero,
            casa_combined_rotated=zero,
            power_bits_match=True,
        )

        self.assertEqual(classification, "tt0-degrid-or-folded-phase-difference")
        self.assertEqual(first, (0, 0))

    def test_exact_classification(self) -> None:
        zero = np.zeros((2, 2), dtype=np.complex64)

        classification, first = subject.classify(
            casars_tt0=zero,
            casars_tt1=zero,
            casars_scaled=zero,
            casars_literal_combined=zero,
            casa_tt0_raw=zero,
            casa_tt1_raw=zero,
            casa_tt1_scaled_raw=zero,
            casa_combined_raw=zero,
            casa_tt0_rotated=zero,
            casa_tt1_rotated=zero,
            casa_tt1_scaled_rotated=zero,
            casa_combined_rotated=zero,
            power_bits_match=True,
        )

        self.assertEqual(classification, "term-separated-prediction-exact")
        self.assertIsNone(first)


if __name__ == "__main__":
    unittest.main()
