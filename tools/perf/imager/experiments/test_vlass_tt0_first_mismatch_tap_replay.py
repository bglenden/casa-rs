#!/usr/bin/env python3

from __future__ import annotations

import unittest

import numpy as np

import vlass_tt0_first_mismatch_tap_replay as subject


class TapReplayArithmeticTests(unittest.TestCase):
    def test_fma_f32_rounds_once(self) -> None:
        left = np.uint32(0x3F80_0001).view(np.float32)
        right = np.uint32(0x3F7F_FFFF).view(np.float32)
        addend = np.float32(-1.0)

        fused = subject.fma_f32(left, right, addend)
        separate = np.float32(np.float32(left * right) + addend)

        self.assertNotEqual(fused.view(np.uint32), separate.view(np.uint32))
        self.assertEqual(int(fused.view(np.uint32)), 0x337F_FFFE)

    def test_first_source_division_variants_match_observed_bits(self) -> None:
        accumulator = subject.bits_to_complex([1033899791, 1036192990])
        normalization = subject.bits_to_complex([1064179348, 3172914251])

        uncontracted = subject.complex_divide_wide_intermediate(
            accumulator, normalization
        )
        metal = subject.complex_divide_fused_numerator(accumulator, normalization)

        self.assertEqual(
            subject.complex_bits(uncontracted),
            [1034097304, 1037600252],
        )
        self.assertEqual(
            subject.complex_bits(metal),
            [1034097304, 1037600253],
        )

    def test_uncontracted_float32_is_the_third_distinct_boundary(self) -> None:
        accumulator = subject.bits_to_complex([1033899791, 1036192990])
        normalization = subject.bits_to_complex([1064179348, 3172914251])

        value = subject.complex_divide(accumulator, normalization)

        self.assertEqual(
            subject.complex_bits(value),
            [1034097304, 1037600251],
        )


if __name__ == "__main__":
    unittest.main()
