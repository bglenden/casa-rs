# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for the reduced VLASS turnaround-fixture staging plan."""

from __future__ import annotations

import pathlib
import tempfile
import unittest

import stage_vlass_turnaround as stage


class StageVlassTurnaroundTests(unittest.TestCase):
    def test_default_frequency_plan_covers_four_ordered_s_band_spws(self) -> None:
        centers = stage.parse_spw_centers("2.2e9,2.7e9,3.2e9,3.7e9")

        self.assertEqual(stage.DEFAULT_SPW_CENTERS_HZ, centers)
        self.assertEqual(
            (2.05e9, 2.2e9, 2.35e9), stage.channel_frequencies(centers[0])
        )
        self.assertEqual(
            (3.55e9, 3.7e9, 3.85e9), stage.channel_frequencies(centers[-1])
        )

    def test_frequency_plan_rejects_non_s_band_or_unordered_centers(self) -> None:
        for value in ("", "1.5e9", "2.7e9,2.2e9", "2.2e9,2.2e9", "wat"):
            with self.subTest(value=value), self.assertRaises(stage.StagingError):
                stage.parse_spw_centers(value)

    def test_row_repetitions_preserve_complete_spw_cycles(self) -> None:
        stage.validate_plan(
            row_repetitions=8,
            imsize=1024,
            cell_arcsec=10.0,
            spw_centers_hz=stage.DEFAULT_SPW_CENTERS_HZ,
        )

        for repetitions in (3, 6):
            with self.subTest(repetitions=repetitions), self.assertRaisesRegex(
                stage.StagingError, "SPW count"
            ):
                stage.validate_plan(
                    row_repetitions=repetitions,
                    imsize=1024,
                    cell_arcsec=10.0,
                    spw_centers_hz=stage.DEFAULT_SPW_CENTERS_HZ,
                )

    def test_product_inventory_is_exact_and_order_independent(self) -> None:
        stage.require_exact_product_inventory(reversed(stage.EXPECTED_PRODUCTS))

        with self.assertRaisesRegex(stage.StagingError, "inventory drifted"):
            stage.require_exact_product_inventory(stage.EXPECTED_PRODUCTS[:-1])

    def test_product_paths_ignore_files_and_unrelated_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = pathlib.Path(tempdir)
            prefix = root / stage.REFERENCE_NAME
            (root / f"{stage.REFERENCE_NAME}.psf.tt0").mkdir()
            (root / f"{stage.REFERENCE_NAME}.image.tt0").mkdir()
            (root / f"{stage.REFERENCE_NAME}.log").write_text("ignored")
            (root / "unrelated.image.tt0").mkdir()

            self.assertEqual(
                [".image.tt0", ".psf.tt0"], stage.collect_product_inventory(prefix)
            )


if __name__ == "__main__":
    unittest.main()
