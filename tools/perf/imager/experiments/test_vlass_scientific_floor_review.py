#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the frozen VLASS scientific-floor reviewer."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np


MODULE_PATH = Path(__file__).with_name("vlass_scientific_floor_review.py")
SPEC = importlib.util.spec_from_file_location("vlass_scientific_floor", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
reviewer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = reviewer
SPEC.loader.exec_module(reviewer)


class VlassScientificFloorReviewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "structure-workspace"
        self.workspace.mkdir()
        self.shape = (32, 32)
        self.comparison_path = self.root / "comparison.json"
        self.comparison_input_path = self.root / "comparison-input.json"
        self.run_log_path = self.root / "run.log"
        self.run_log_path.write_text("bounded synthetic run\n", encoding="utf-8")
        self.alpha_threshold = 1.0e-3
        self.source_region = {
            "id": "synthetic-source",
            "products": [".image.tt0", ".model.tt0", ".residual.tt0"],
            "blc": [10, 10],
            "trc": [21, 21],
        }
        self.arrays = self._write_planes()
        self.comparison = self._comparison()
        self.comparison_input = {
            "request_sha256": "a" * 64,
            "products": list(reviewer.EXPECTED_PRODUCTS),
        }
        self.comparison_path.write_text(
            json.dumps(self.comparison),
            encoding="utf-8",
        )
        self.comparison_input_path.write_text(
            json.dumps(self.comparison_input),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_planes(self) -> dict[str, tuple[np.ndarray, np.ndarray]]:
        generator = np.random.default_rng(7)
        base_noise = generator.normal(0.0, 1.0e-4, size=self.shape)
        x, y = np.indices(self.shape)
        source = 1.0e-2 * np.exp(-((x - 16.0) ** 2 + (y - 16.0) ** 2) / (2.0 * 2.0**2))
        right_planes = {
            ".image.tt0": base_noise + source,
            ".image.tt1": 0.5 * base_noise + 0.2 * source,
            ".residual.tt0": base_noise,
            ".residual.tt1": 0.5 * base_noise,
            ".alpha": np.full(self.shape, 0.2, dtype=np.float64),
            ".alpha.error": np.full(self.shape, 0.05, dtype=np.float64),
        }
        arrays: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        for suffix in reviewer.REVIEW_PLANE_PRODUCTS:
            right = np.asarray(right_planes[suffix], dtype=np.float64)
            left = right.copy()
            directory = reviewer.structure_product_workspace(self.workspace, suffix)
            directory.mkdir()
            left.tofile(directory / "left.f64")
            right.tofile(directory / "right.f64")
            np.ones(self.shape, dtype=np.uint8).tofile(directory / "coverage.u8")
            arrays[suffix] = (left, right)
        return arrays

    def _comparison(self) -> dict[str, object]:
        products: dict[str, object] = {}
        for suffix in reviewer.EXPECTED_PRODUCTS:
            full_array: dict[str, object] = {
                "shape": [*self.shape, 1, 1],
                "coverage_complete": True,
                "diff_rms_over_right_rms": 0.0,
                "diff_abs_max_over_right_peak": 0.0,
                "diff_abs_max": 0.0,
                "comparison_domain_count": self.shape[0] * self.shape[1],
                "left_peak_abs": {
                    "abs_value": 1.0e-2,
                    "location": [16, 16, 0, 0],
                },
                "right_peak_abs": {
                    "abs_value": 1.0e-2,
                    "location": [16, 16, 0, 0],
                },
            }
            if suffix in {".alpha", ".alpha.error"}:
                full_array["topology"] = {
                    "mask_mismatch_count": 0,
                    "mask_mismatch_samples": [],
                }
            if suffix in reviewer.COHERENT_PRODUCTS:
                full_array["structured_difference"] = {
                    "beam_block_rms_by_scale": [
                        {
                            "beam_width_multiplier": multiplier,
                            "block_side_pixels": int(2 * multiplier),
                            "block_mean_rms": 0.0,
                        }
                        for multiplier in (1.0, 2.0, 4.0, 8.0, 16.0)
                    ],
                    "large_scale_power_fraction": {"fraction": 0.0},
                    "low_order_r2_quadratic": 0.0,
                }
            products[suffix] = {
                "status": "compared",
                "metadata": {"parity": True},
                "full_array": full_array,
            }

        image_left, image_right = self.arrays[".image.tt0"]
        selection = (
            slice(self.source_region["blc"][0], self.source_region["trc"][0] + 1),
            slice(self.source_region["blc"][1], self.source_region["trc"][1] + 1),
        )
        integrated_sum = float(np.sum(image_right[selection]))
        source_result = {
            **self.source_region,
            "left": {
                "peak_abs": {
                    "abs_value": float(np.max(image_left[selection])),
                    "location": [16, 16],
                },
                "integrated_flux": integrated_sum / 4.0,
                "centroid_pixels": [16.0, 16.0],
            },
            "right": {
                "peak_abs": {
                    "abs_value": float(np.max(image_right[selection])),
                    "location": [16, 16],
                },
                "integrated_flux": integrated_sum / 4.0,
                "centroid_pixels": [16.0, 16.0],
            },
        }
        products[".image.tt0"]["source_regions"] = [source_result]
        # A localized difference at about two percent of TT1 noise is well
        # below the artifact guard and must not override the beam-scale gates.
        products[".image.tt1"]["full_array"]["diff_abs_max"] = 1.0e-6
        return {
            "status": "completed",
            "comparison_mode": "full",
            "request_sha256": "a" * 64,
            "requested_products": list(reviewer.EXPECTED_PRODUCTS),
            "require_exact_product_inventory": True,
            "require_metadata_parity": True,
            "product_inventory": {"status": "matched"},
            "source_regions": [self.source_region],
            "beam_info": {
                "beam_area_pixels": 4.0,
                "fwhm_pixels": [2, 2],
            },
            "products": products,
        }

    def test_complete_frozen_candidate_passes(self) -> None:
        receipt = reviewer.build_review(
            comparison_path=self.comparison_path,
            comparison_input_path=self.comparison_input_path,
            run_log_path=self.run_log_path,
            workspace_root=self.workspace,
            alpha_threshold=self.alpha_threshold,
            panel_path=None,
        )
        self.assertEqual(receipt["status"], "passed")
        self.assertEqual(receipt["decision"], "promote")
        self.assertTrue(all(receipt["gates"].values()))
        self.assertFalse(receipt["scope"]["runs_casa"])
        self.assertFalse(receipt["scope"]["runs_imaging"])

    def test_non_boundary_alpha_topology_difference_fails(self) -> None:
        for suffix in (".alpha", ".alpha.error"):
            topology = self.comparison["products"][suffix]["full_array"]["topology"]
            topology["mask_mismatch_count"] = 1
            topology["mask_mismatch_samples"] = [
                {
                    "location": [16, 16, 0, 0],
                    "left_mask": True,
                    "right_mask": False,
                    "left_value": 0.2,
                    "right_value": 0.0,
                }
            ]
            self.comparison["products"][suffix]["status"] = "topology_mismatch"
        self.comparison_path.write_text(
            json.dumps(self.comparison),
            encoding="utf-8",
        )
        receipt = reviewer.build_review(
            comparison_path=self.comparison_path,
            comparison_input_path=self.comparison_input_path,
            run_log_path=self.run_log_path,
            workspace_root=self.workspace,
            alpha_threshold=self.alpha_threshold,
            panel_path=None,
        )
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["decision"], "hold")
        self.assertIn(
            "alpha topology mismatch is not confined to cutoff boundary",
            receipt["failures"],
        )

    def test_conspicuous_localized_difference_fails(self) -> None:
        self.comparison["products"][".image.tt1"]["full_array"]["diff_abs_max"] = 1.0e-4
        self.comparison_path.write_text(
            json.dumps(self.comparison),
            encoding="utf-8",
        )
        receipt = reviewer.build_review(
            comparison_path=self.comparison_path,
            comparison_input_path=self.comparison_input_path,
            run_log_path=self.run_log_path,
            workspace_root=self.workspace,
            alpha_threshold=self.alpha_threshold,
            panel_path=None,
        )
        self.assertEqual(receipt["status"], "failed")
        self.assertIn(
            ".image.tt1 maximum difference over noise",
            receipt["failures"],
        )

    def test_comparison_input_hash_mismatch_fails_closed(self) -> None:
        self.comparison_input["request_sha256"] = "b" * 64
        self.comparison_input_path.write_text(
            json.dumps(self.comparison_input),
            encoding="utf-8",
        )
        receipt = reviewer.build_review(
            comparison_path=self.comparison_path,
            comparison_input_path=self.comparison_input_path,
            run_log_path=self.run_log_path,
            workspace_root=self.workspace,
            alpha_threshold=self.alpha_threshold,
            panel_path=None,
        )
        self.assertEqual(receipt["status"], "failed")
        self.assertIn(
            "comparison request hash does not match its input",
            receipt["failures"],
        )


if __name__ == "__main__":
    unittest.main()
