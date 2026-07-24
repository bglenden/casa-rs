# SPDX-License-Identifier: LGPL-3.0-or-later
"""Boundary tests for frozen imaging-product numerical tolerances."""

from __future__ import annotations

import copy
import math
import unittest

from perf_harness.tolerances import (
    ToleranceContractError,
    evaluate_comparison_tolerances,
    validate_tolerance_contract,
)


class ImagingToleranceTests(unittest.TestCase):
    def test_every_hard_ceiling_passes_at_boundary_and_fails_just_outside(self) -> None:
        contract = make_contract()
        comparison = make_comparison()
        at_boundary = evaluate_comparison_tolerances(comparison, contract)
        self.assertEqual("passed", at_boundary["status"])

        mutations = {
            "diff_rms": lambda value: value["products"][".image.tt0"].update(
                diff_rms_over_right_rms=0.0010001
            ),
            "diff_max": lambda value: value["products"][".image.tt0"].update(
                diff_abs_max_over_right_peak=0.0050001
            ),
            "peak": lambda value: value["products"][".image.tt0"]["source_regions"][0][
                "left"
            ]["peak_abs"].update(abs_value=10.010001),
            "beam_major": lambda value: value["products"][".image.tt0"]["metadata"][
                "left"
            ]["restoring_beam"]["major"].update(value=2.0020002),
            "beam_minor": lambda value: value["products"][".image.tt0"]["metadata"][
                "left"
            ]["restoring_beam"]["minor"].update(value=1.0010001),
            "beam_pa": lambda value: value["products"][".image.tt0"]["metadata"][
                "left"
            ]["restoring_beam"]["positionangle"].update(value=10.1001),
            "centroid": lambda value: value["products"][".image.tt0"]["source_regions"][
                0
            ]["left"].update(centroid_pixels=[4.0501, 8.0]),
            "integrated": lambda value: value["products"][".image.tt0"][
                "source_regions"
            ][0]["left"].update(integrated_flux=20.020002),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                outside = copy.deepcopy(comparison)
                mutate(outside)
                self.assertEqual(
                    "failed",
                    evaluate_comparison_tolerances(outside, contract)["status"],
                )

    def test_missing_source_region_or_sampled_comparison_is_incomplete(self) -> None:
        comparison = make_comparison()
        comparison["comparison_mode"] = "sampled"
        comparison["products"][".image.tt0"].pop("source_regions")

        result = evaluate_comparison_tolerances(comparison, make_contract())

        self.assertEqual("incomplete", result["status"])
        self.assertIn("comparison_mode", result["incomplete_checks"])
        self.assertTrue(
            any(
                "centroid_pixels" in name or "integrated_flux_relative" in name
                for name in result["incomplete_checks"]
            )
        )

    def test_beam_quantities_are_compared_after_unit_conversion(self) -> None:
        comparison = make_comparison()
        beam = comparison["products"][".image.tt0"]["metadata"]["left"][
            "restoring_beam"
        ]
        beam["major"] = {"value": 2.002 / 3600.0, "unit": "deg"}
        beam["positionangle"] = {
            "value": math.radians(10.1),
            "unit": "rad",
        }

        self.assertEqual(
            "passed",
            evaluate_comparison_tolerances(comparison, make_contract())["status"],
        )

    def test_contract_rejects_unknown_or_negative_thresholds(self) -> None:
        contract = make_contract()
        contract["default"]["magic"] = 1.0
        with self.assertRaises(ToleranceContractError):
            validate_tolerance_contract(contract)

        contract = make_contract()
        contract["default"] = {"require_topology_parity": False}
        with self.assertRaises(ToleranceContractError):
            validate_tolerance_contract(contract)

        contract = make_contract()
        contract["products"][".image.tt0"] = {"allowed_structure_labels": ["unknown"]}
        with self.assertRaisesRegex(ToleranceContractError, "known-label list"):
            validate_tolerance_contract(contract)

    def test_source_region_inventory_is_exact(self) -> None:
        missing = make_comparison()
        missing["products"][".image.tt0"]["source_regions"] = []
        result = evaluate_comparison_tolerances(missing, make_contract())
        self.assertEqual("incomplete", result["status"])

        extra = make_comparison()
        extra["products"][".image.tt0"]["source_regions"].append(
            {
                "id": "unfrozen-source",
                "left": {},
                "right": {},
            }
        )
        result = evaluate_comparison_tolerances(extra, make_contract())
        self.assertEqual("failed", result["status"])
        self.assertIn(".image.tt0.source_region_inventory", result["failed_checks"])

    def test_source_box_and_structure_labels_must_match_the_frozen_evidence(
        self,
    ) -> None:
        moved_box = make_comparison()
        moved_box["products"][".image.tt0"]["source_regions"][0]["trc"] = [
            11,
            10,
        ]
        result = evaluate_comparison_tolerances(moved_box, make_contract())
        self.assertEqual("failed", result["status"])
        self.assertIn(
            ".image.tt0.source[source-1].contract",
            result["failed_checks"],
        )

        disagreeing_review = make_comparison()
        disagreeing_review["products"][".image.tt0"]["structured_difference"]["review"][
            "label"
        ] = "not_applicable_exact_zero"
        result = evaluate_comparison_tolerances(disagreeing_review, make_contract())
        self.assertEqual("failed", result["status"])
        self.assertIn(".image.tt0.structured_difference", result["failed_checks"])

    def test_vacuous_contract_cannot_pass(self) -> None:
        contract = {
            "contract_version": 1,
            "require_full_array": False,
            "default": {},
            "products": {},
        }
        result = evaluate_comparison_tolerances(make_comparison(), contract)
        self.assertEqual("incomplete", result["status"])

        contract = make_contract()
        contract["default"]["diff_rms_over_right_rms"] = -1.0
        with self.assertRaises(ToleranceContractError):
            validate_tolerance_contract(contract)


def make_contract() -> dict:
    return {
        "contract_version": 1,
        "require_full_array": True,
        "default": {
            "beam_major_relative": 0.001,
            "beam_minor_relative": 0.001,
            "beam_pa_degrees": 0.1,
            "centroid_pixels": 0.05,
            "diff_abs_max_over_right_peak": 0.005,
            "diff_rms_over_right_rms": 0.001,
            "integrated_flux_relative": 0.001,
            "peak_relative": 0.001,
            "require_topology_parity": True,
            "allowed_structure_labels": ["good"],
        },
        "products": {".image.tt0": {}},
    }


def make_comparison() -> dict:
    return {
        "status": "completed",
        "comparison_mode": "full",
        "source_regions": [
            {
                "id": "source-1",
                "products": [".image.tt0"],
                "blc": [0, 0],
                "trc": [10, 10],
            }
        ],
        "products": {
            ".image.tt0": {
                "status": "compared",
                "diff_rms_over_right_rms": 0.001,
                "diff_abs_max_over_right_peak": 0.005,
                "left_peak_abs": {"abs_value": 10.01},
                "right_peak_abs": {"abs_value": 10.0},
                "topology_parity": True,
                "metadata": {
                    "left": {
                        "restoring_beam": {
                            "major": {"value": 2.002, "unit": "arcsec"},
                            "minor": {"value": 1.001, "unit": "arcsec"},
                            "positionangle": {"value": 10.1, "unit": "deg"},
                        }
                    },
                    "right": {
                        "restoring_beam": {
                            "major": {"value": 2.0, "unit": "arcsec"},
                            "minor": {"value": 1.0, "unit": "arcsec"},
                            "positionangle": {"value": 10.0, "unit": "deg"},
                        }
                    },
                },
                "source_regions": [
                    {
                        "id": "source-1",
                        "products": [".image.tt0"],
                        "blc": [0, 0],
                        "trc": [10, 10],
                        "left": {
                            "status": "measured",
                            "centroid_pixels": [4.05, 8.0],
                            "integrated_flux": 20.02,
                            "peak_abs": {"abs_value": 10.01},
                        },
                        "right": {
                            "status": "measured",
                            "centroid_pixels": [4.0, 8.0],
                            "integrated_flux": 20.0,
                            "peak_abs": {"abs_value": 10.0},
                        },
                    }
                ],
                "structured_difference": {
                    "status": "computed",
                    "classification": {"overall": "good"},
                    "review": {"label": "good"},
                },
            }
        },
    }


if __name__ == "__main__":
    unittest.main()
