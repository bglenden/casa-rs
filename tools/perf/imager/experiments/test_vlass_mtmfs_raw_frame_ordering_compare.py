#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np


MODULE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(MODULE_DIR))
SPEC = importlib.util.spec_from_file_location(
    "vlass_mtmfs_raw_frame_ordering_compare",
    MODULE_DIR / "vlass_mtmfs_raw_frame_ordering_compare.py",
)
assert SPEC is not None and SPEC.loader is not None
subject = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(subject)


class RawFrameOrderingCompareTest(unittest.TestCase):
    def test_binary_layout_matches_helper_contract(self) -> None:
        self.assertEqual(subject.INPUT_DTYPE.itemsize, 48)
        self.assertEqual(subject.OUTPUT_DTYPE.itemsize, 152)

    def test_classification_requires_both_casa_order_boundaries(self) -> None:
        self.assertEqual(
            subject.classify_ordering(
                controls_valid=False,
                raw_order_scaled_mismatches=0,
                raw_order_combined_mismatches=0,
            ),
            "evidence-or-control-invalid",
        )
        self.assertEqual(
            subject.classify_ordering(
                controls_valid=True,
                raw_order_scaled_mismatches=1,
                raw_order_combined_mismatches=0,
            ),
            "raw-frame-scale-different",
        )
        self.assertEqual(
            subject.classify_ordering(
                controls_valid=True,
                raw_order_scaled_mismatches=0,
                raw_order_combined_mismatches=1,
            ),
            "raw-frame-scale-exact-combined-different",
        )
        self.assertEqual(
            subject.classify_ordering(
                controls_valid=True,
                raw_order_scaled_mismatches=0,
                raw_order_combined_mismatches=0,
            ),
            "raw-frame-scale-add-closes-all",
        )

    def test_rust_helper_matches_separate_float_reference_graphs(self) -> None:
        frequencies = np.asarray(
            [1_986_926_888.2885466, 1_998_926_446.748266],
            dtype=np.float64,
        )
        raw_tt0 = np.asarray(
            [
                [complex(0.125, -0.25), complex(-0.5, 0.75)],
                [complex(0.03125, 0.5), complex(-0.125, -0.375)],
            ],
            dtype=np.complex64,
        )
        raw_tt1 = np.asarray(
            [
                [complex(0.2, 0.1), complex(-0.3, 0.4)],
                [complex(-0.7, 0.05), complex(0.6, -0.2)],
            ],
            dtype=np.complex64,
        )
        phase_values = np.asarray(
            [
                complex(np.float32(0.99999994), np.float32(-0.0002)),
                complex(np.float32(0.9999998), np.float32(0.0006)),
            ],
            dtype=np.complex64,
        )
        source_trace = {
            "samples": [
                {
                    "source_ordinal": ordinal,
                    "row_id": ordinal,
                    "ddid": 2,
                    "spw_id": 2,
                    "channel": ordinal,
                    "frequency_hz": float(frequencies[ordinal]),
                    "phase_re_bits": int(
                        np.float32(phase_values[ordinal].real).view(np.uint32)
                    ),
                    "phase_im_bits": int(
                        np.float32(phase_values[ordinal].imag).view(np.uint32)
                    ),
                }
                for ordinal in range(2)
            ]
        }
        records = subject.build_input_records(
            frequencies=frequencies,
            raw_tt0=raw_tt0,
            raw_tt1=raw_tt1,
            source_trace=source_trace,
        )
        reference_bits = subject.EXPECTED_REFERENCE_BITS
        reference = subject.taylor.f64_from_bits(reference_bits)
        powers = np.asarray(
            [
                np.float32(
                    (np.float64(np.float32(frequency)) - reference) / reference
                )
                for frequency in frequencies
            ],
            dtype=np.float32,
        )

        with tempfile.TemporaryDirectory(prefix="vlass-ordering-test-") as directory:
            temporary = Path(directory)
            helper, metadata = subject.compile_helper(
                rustc="rustc",
                source=MODULE_DIR / "vlass_mtmfs_raw_frame_ordering.rs",
                temporary=temporary,
            )
            output, _ = subject.run_helper(
                helper=helper,
                records=records,
                reference_bits=reference_bits,
                temporary=temporary,
            )

        self.assertEqual(metadata["schema"], subject.HELPER_SCHEMA)
        self.assertTrue(
            np.array_equal(
                output["power_f32_bits"],
                powers.view(np.uint32),
            )
        )
        aligned_tt0 = subject.term_compare.phase_rotate_pairs(
            raw_tt0,
            source_trace,
        )
        aligned_tt1 = subject.term_compare.phase_rotate_pairs(
            raw_tt1,
            source_trace,
        )
        scaled_current = subject.taylor.separately_scale_pairs(
            aligned_tt1,
            powers,
        )
        combined_current = subject.taylor.separately_add_pairs(
            aligned_tt0,
            scaled_current,
        )
        scaled_raw = subject.taylor.separately_scale_pairs(raw_tt1, powers)
        combined_raw = subject.taylor.separately_add_pairs(raw_tt0, scaled_raw)
        aligned_scaled_raw = subject.term_compare.phase_rotate_pairs(
            scaled_raw,
            source_trace,
        )
        aligned_combined_raw = subject.term_compare.phase_rotate_pairs(
            combined_raw,
            source_trace,
        )
        expected = {
            "aligned_tt0_bits": aligned_tt0,
            "aligned_tt1_bits": aligned_tt1,
            "scaled_current_bits": scaled_current,
            "combined_current_bits": combined_current,
            "scaled_raw_bits": scaled_raw,
            "combined_raw_bits": combined_raw,
            "aligned_scaled_raw_bits": aligned_scaled_raw,
            "aligned_combined_raw_bits": aligned_combined_raw,
        }
        for field, values in expected.items():
            with self.subTest(field=field):
                self.assertTrue(
                    np.array_equal(
                        subject.pair_bits(values),
                        output[field],
                    )
                )


if __name__ == "__main__":
    unittest.main()
