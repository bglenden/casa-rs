# SPDX-License-Identifier: LGPL-3.0-or-later
"""Adversarial validation tests for authoritative full-array comparisons."""

from __future__ import annotations

import copy
import math
import unittest

from perf_harness import casa_image_compare as comparator
from perf_harness.image_compare import (
    comparison_request_binding,
    normalize_comparison_request,
    validate_comparison_output,
)


SUFFIX = ".image.tt0"


class FullArrayOutputValidationTests(unittest.TestCase):
    def test_complete_full_array_output_is_accepted(self) -> None:
        request = normalized_request()

        validate_comparison_output(comparison_output(request), request)

    def test_v4_output_without_optional_mask_mismatch_samples_is_accepted(self) -> None:
        request = normalized_request()
        candidate = comparison_output(request)
        topology(candidate).pop("mask_mismatch_samples")

        validate_comparison_output(candidate, request)

    def test_coverage_chunk_topology_and_algebra_mutations_fail_closed(self) -> None:
        request = normalized_request()
        mutations = {
            "missing full field": lambda value: full(value).pop("total_elements"),
            "wrong total": lambda value: full(value).__setitem__("total_elements", 5),
            "noninteger total": lambda value: full(value).__setitem__(
                "total_elements", 4.0
            ),
            "coverage flag lie": lambda value: full(value).__setitem__(
                "coverage_complete", False
            ),
            "incomplete coverage": incomplete_coverage,
            "wrong chunk budget": lambda value: full(value).__setitem__(
                "full_chunk_elements", 4
            ),
            "oversized chunk": lambda value: full(value).__setitem__(
                "max_chunk_elements_observed", 4
            ),
            "impossible chunk count": lambda value: full(value).__setitem__(
                "chunks", 1
            ),
            "missing topology field": lambda value: topology(value).pop(
                "mask_mismatch_count"
            ),
            "mask flag lie": lambda value: topology(value).__setitem__(
                "mask_equal", False
            ),
            "impossible mask regions": lambda value: topology(value).__setitem__(
                "left_masked_count", 1
            ),
            "comparison count not from finite topology": lambda value: topology(
                value
            ).__setitem__("left_finite_count", 3),
            "nonfinite counts not exhaustive": lambda value: topology(value)[
                "left_nonfinite"
            ].__setitem__("nan", 1),
            "rms not from sum squares": lambda value: full(value)["left"].__setitem__(
                "rms", 0.0
            ),
            "covariance not from streamed sums": lambda value: full(value).__setitem__(
                "covariance", 0.0
            ),
            "difference nested mirror lie": lambda value: full(value)[
                "difference"
            ].__setitem__("integrated_value", 0.0),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                candidate = comparison_output(request)
                mutate(candidate)
                with self.assertRaises(ValueError):
                    validate_comparison_output(candidate, request)

    def test_reproduced_top_level_false_green_cannot_hide_full_array_metrics(
        self,
    ) -> None:
        request = normalized_request()
        candidate = comparison_output(request)
        product = candidate["products"][SUFFIX]
        self.assertGreater(product["full_array"]["diff_rms_over_right_rms"], 0.1)

        product["diff_rms_over_right_rms"] = 0.0
        product["diff_abs_max_over_right_peak"] = 0.0

        with self.assertRaisesRegex(ValueError, "authoritative full_array"):
            validate_comparison_output(candidate, request)

    def test_topology_and_peak_mirrors_cannot_diverge_from_full_array(self) -> None:
        request = normalized_request()
        mutations = {
            "finite overlap": lambda product: product.__setitem__("finite_overlap", 3),
            "topology parity": lambda product: product.__setitem__(
                "topology_parity", False
            ),
            "left minimum": lambda product: product.__setitem__("left_min", 99.0),
            "left peak": lambda product: product["left_peak_abs"].__setitem__(
                "value", 4.0
            ),
            "correlation": lambda product: product.__setitem__("correlation", 0.0),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                candidate = comparison_output(request)
                mutate(candidate["products"][SUFFIX])
                with self.assertRaises(ValueError):
                    validate_comparison_output(candidate, request)

    def test_legacy_aliases_are_required_and_rebound_when_requested(self) -> None:
        request = normalized_request(legacy=True)
        candidate = comparison_output(request)
        validate_comparison_output(candidate, request)

        candidate["products"][SUFFIX]["diff_rms_over_casa_rms"] = 0.0
        with self.assertRaisesRegex(ValueError, "legacy alias"):
            validate_comparison_output(candidate, request)

    def test_completed_status_cannot_bypass_incomplete_product_evidence(self) -> None:
        request = normalized_request()
        candidate = comparison_output(request)
        product = candidate["products"][SUFFIX]
        product["status"] = "topology_mismatch"
        product.pop("full_array")
        product.pop("structured_difference")

        with self.assertRaisesRegex(ValueError, "status/reason is not derived"):
            validate_comparison_output(candidate, request)

    def test_difference_norm_is_bound_to_operands_and_cross_sum(self) -> None:
        request = normalized_request()
        candidate = comparison_output(request)
        forge_zero_difference_for_opposite_operands(candidate)

        with self.assertRaisesRegex(ValueError, "difference sum_squares"):
            validate_comparison_output(candidate, request)

    def test_nonfinite_kind_parity_is_bound_to_categorical_marginals(self) -> None:
        request = normalized_request()
        candidate = comparison_output(request)
        forge_impossible_nonfinite_kind_parity(candidate)

        with self.assertRaisesRegex(ValueError, "categorical marginals"):
            validate_comparison_output(candidate, request)

    def test_nested_metadata_and_structure_results_cannot_forge_a_green_review(
        self,
    ) -> None:
        request = normalized_request()

        bad_metadata = comparison_output(request)
        bad_metadata["products"][SUFFIX]["metadata"] = {
            "status": "matched",
            "parity": True,
        }
        with self.assertRaisesRegex(ValueError, "unrequested metadata"):
            validate_comparison_output(bad_metadata, request)

        bad_classification = comparison_output(request)
        structure = bad_classification["products"][SUFFIX]["structured_difference"]
        structure["classification"]["overall"] = "good"
        bad_classification["products"][SUFFIX]["full_array"][
            "structured_difference"
        ] = copy.deepcopy(structure)
        with self.assertRaisesRegex(ValueError, "classification is not derived"):
            validate_comparison_output(bad_classification, request)

        bad_review = comparison_output(request)
        structure = bad_review["products"][SUFFIX]["structured_difference"]
        structure["review"]["checks"][0]["value"] = 0.0
        bad_review["products"][SUFFIX]["full_array"]["structured_difference"] = (
            copy.deepcopy(structure)
        )
        with self.assertRaisesRegex(ValueError, "review is not derived"):
            validate_comparison_output(bad_review, request)

    def test_structured_difference_payload_mutations_fail_closed(self) -> None:
        request = normalized_request()

        def set_structure(candidate: dict[str, object], structure: dict) -> None:
            product = candidate["products"][SUFFIX]  # type: ignore[index]
            product["structured_difference"] = structure
            product["full_array"]["structured_difference"] = copy.deepcopy(  # type: ignore[index]
                structure
            )

        mutations = {
            "negative analysis_pixels": lambda structure: structure.__setitem__(
                "analysis_pixels", -999
            ),
            "missing mask": lambda structure: structure.pop("mask"),
            "forged scale fit": lambda structure: structure.__setitem__(
                "scale_offset_gradient_fit",
                {
                    "status": "computed",
                    "fit_pixels": -1,
                    "coefficients": [1.0e99],
                },
            ),
            "forged block scales": lambda structure: structure.__setitem__(
                "beam_block_rms_by_scale", [{"n_blocks": -4}]
            ),
            "unknown structure field": lambda structure: structure.__setitem__(
                "forged", True
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                candidate = comparison_output(request)
                structure = copy.deepcopy(
                    candidate["products"][SUFFIX]["structured_difference"]  # type: ignore[index]
                )
                mutate(structure)
                set_structure(candidate, structure)
                with self.assertRaises(ValueError):
                    validate_comparison_output(candidate, request)

    def test_top_level_structured_difference_review_is_exactly_derived(self) -> None:
        request = normalized_request()

        missing = comparison_output(request)
        missing.pop("structured_difference_review")
        with self.assertRaisesRegex(ValueError, "structured_difference_review"):
            validate_comparison_output(missing, request)

        arbitrary = comparison_output(request)
        arbitrary["structured_difference_review"] = {
            "label": "bad",
            "summary": "forged",
            "products": {},
        }
        with self.assertRaisesRegex(ValueError, "structured_difference_review"):
            validate_comparison_output(arbitrary, request)


def normalized_request(*, legacy: bool = False) -> dict[str, object]:
    prefixes = (
        {"rust_prefix": "/evidence/left", "casa_prefix": "/evidence/right"}
        if legacy
        else {"left_prefix": "/evidence/left", "right_prefix": "/evidence/right"}
    )
    return normalize_comparison_request(
        {
            "mode": "full",
            **prefixes,
            "products": [SUFFIX],
            "max_elements_per_product": 4,
            "full_chunk_elements": 3,
            "require_exact_product_inventory": True,
            "require_metadata_parity": False,
            "source_regions": [],
            "tolerances": None,
            "panel_dir": "/evidence/panels",
            "structure_workspace_dir": "/evidence/structure-workspace",
        }
    )


def comparison_output(request: dict[str, object]) -> dict[str, object]:
    beam = native_beam()
    structure = full_structure(beam)
    left_peak = peak([1, 1], 5.0)
    right_peak = peak([1, 1], 4.0)
    diff_peak = peak([0, 0], 1.0)
    left_rms = math.sqrt(13.5)
    right_rms = math.sqrt(7.5)
    rms_ratio = 1.0 / right_rms
    full_array = {
        "status": "compared",
        "shape": [2, 2],
        "full_chunk_elements": 3,
        "chunks": 2,
        "max_chunk_elements_observed": 3,
        "total_elements": 4,
        "elements_visited": 4,
        "coverage_complete": True,
        "comparison_domain": "left_and_right_pixel_masks_and_finite_values",
        "count": 4,
        "comparison_domain_count": 4,
        "topology": {
            "mask_equal": True,
            "mask_mismatch_count": 0,
            "mask_mismatch_samples": [],
            "left_masked_count": 0,
            "right_masked_count": 0,
            "finite_equal": True,
            "finite_topology_mismatch_count": 0,
            "nonfinite_kind_equal": True,
            "nonfinite_kind_mismatch_count": 0,
            "left_finite_count": 4,
            "right_finite_count": 4,
            "left_nonfinite": nonfinite_counts(),
            "right_nonfinite": nonfinite_counts(),
        },
        "left": operand(2.0, 5.0, 14.0, 54.0, left_rms, left_peak),
        "right": operand(1.0, 4.0, 10.0, 30.0, right_rms, right_peak),
        "cross_sum": 40.0,
        "covariance": 1.25,
        "correlation": 1.0,
        "left_integrated_value": 14.0,
        "right_integrated_value": 10.0,
        "diff_integrated_value": 4.0,
        "left_peak_abs": copy.deepcopy(left_peak),
        "right_peak_abs": copy.deepcopy(right_peak),
        "diff_peak_abs": copy.deepcopy(diff_peak),
        "difference": {
            "sum": 4.0,
            "sum_squares": 4.0,
            "integrated_value": 4.0,
            "rms": 1.0,
            "abs_max": 1.0,
            "peak_abs": copy.deepcopy(diff_peak),
        },
        "diff_rms": 1.0,
        "diff_abs_max": 1.0,
        "diff_rms_over_right_rms": rms_ratio,
        "diff_abs_max_over_right_peak": 0.25,
        "structured_difference": copy.deepcopy(structure),
    }
    product = {
        "status": "compared",
        "left_path": str(request["left_prefix"]) + SUFFIX,
        "right_path": str(request["right_prefix"]) + SUFFIX,
        "shape": [2, 2],
        "finite_overlap": 4,
        "topology_parity": True,
        "left_min": 2.0,
        "left_max": 5.0,
        "left_rms": left_rms,
        "right_min": 1.0,
        "right_max": 4.0,
        "right_rms": right_rms,
        "left_peak_abs": copy.deepcopy(left_peak),
        "right_peak_abs": copy.deepcopy(right_peak),
        "diff_peak_abs": copy.deepcopy(diff_peak),
        "diff_abs_max": 1.0,
        "diff_rms": 1.0,
        "diff_rms_over_right_rms": rms_ratio,
        "diff_abs_max_over_right_peak": 0.25,
        "correlation": 1.0,
        "metadata_parity_required": False,
        "metadata": {"status": "not_required", "parity": None},
        "structured_difference": copy.deepcopy(structure),
        "full_array": full_array,
    }
    if request["legacy_operand_aliases"]:
        product.update(
            {
                "rust_min": product["left_min"],
                "rust_max": product["left_max"],
                "rust_rms": product["left_rms"],
                "casa_min": product["right_min"],
                "casa_max": product["right_max"],
                "casa_rms": product["right_rms"],
                "diff_rms_over_casa_rms": product["diff_rms_over_right_rms"],
                "diff_abs_max_over_casa_peak": product["diff_abs_max_over_right_peak"],
                "rust_peak_abs": copy.deepcopy(product["left_peak_abs"]),
                "casa_peak_abs": copy.deepcopy(product["right_peak_abs"]),
            }
        )
    products = {SUFFIX: product}
    inventory = {
        "status": "matched",
        "required": True,
        "observed_match": True,
        "expected": [SUFFIX],
        "left": [SUFFIX],
        "right": [SUFFIX],
        "left_missing": [],
        "left_extra": [],
        "right_missing": [],
        "right_extra": [],
        "left_right_equal": True,
    }
    return {
        "schema_version": request["schema_version"],
        "request_binding": copy.deepcopy(comparison_request_binding(request)),
        "request_sha256": request["request_sha256"],
        "status": "completed",
        "comparison_mode": "full",
        "max_elements_per_product": request["max_elements_per_product"],
        "full_chunk_elements": request["full_chunk_elements"],
        "left_prefix": request["left_prefix"],
        "right_prefix": request["right_prefix"],
        "left_label": request["left_label"],
        "right_label": request["right_label"],
        "requested_products": [SUFFIX],
        "require_exact_product_inventory": True,
        "require_metadata_parity": False,
        "legacy_operand_aliases": request["legacy_operand_aliases"],
        "source_regions": [],
        "tolerances": None,
        "panel_dir": request["panel_dir"],
        "structure_workspace_dir": request["structure_workspace_dir"],
        "beam_info": copy.deepcopy(beam),
        "products": products,
        "product_inventory": inventory,
        "structured_difference_review": comparator.summarize_product_reviews(products),
    }


def operand(
    minimum: float,
    maximum: float,
    value_sum: float,
    sum_squares: float,
    rms: float,
    peak_value: dict[str, object],
) -> dict[str, object]:
    return {
        "min": minimum,
        "max": maximum,
        "sum": value_sum,
        "sum_squares": sum_squares,
        "rms": rms,
        "integrated_value": value_sum,
        "peak_abs": copy.deepcopy(peak_value),
    }


def peak(location: list[int], value: float) -> dict[str, object]:
    return {"location": location, "value": value, "abs_value": abs(value)}


def nonfinite_counts() -> dict[str, int]:
    return {"nan": 0, "positive_infinity": 0, "negative_infinity": 0}


def native_beam() -> dict[str, object]:
    return {
        "status": "estimated_from_psf",
        "estimation_method": (
            "streamed_native_central_plane_peak_and_native_cross_sections"
        ),
        "coordinate_domain": "native_direction_pixels",
        "native_plane_coverage": {
            "pixels_visited": 4,
            "expected_pixels": 4,
            "coverage_complete": True,
        },
    }


def full_structure(beam: dict[str, object]) -> dict[str, object]:
    diff_rms = 1.0
    normalization = math.sqrt(7.5)
    normalized_diff_rms = diff_rms / normalization
    low_order_r2 = 0.0
    large_scale_power = {
        "min_wavelength_beams": 8.0,
        "frequency_cutoff_cycles_per_pixel": 0.125,
        "fraction": 0.0,
    }
    block_decay = None
    classification = comparator.structured_difference_classification(
        normalized_diff_rms,
        low_order_r2,
        large_scale_power,
        block_decay,
    )
    review = comparator.structured_difference_review(
        SUFFIX,
        classification,
        normalized_diff_rms,
        low_order_r2,
        large_scale_power,
        block_decay,
    )
    return {
        "status": "computed",
        "evidence_scope": "full_native_central_spatial_plane_disk_backed",
        "native_spatial_evidence": {
            "method": "exact_native_central_plane_disk_backed_memmap",
            "source_shape": [2, 2],
            "storage": "temporary_disk_backed_native_arrays",
            "array_count": 4,
            "temporary_bytes": 100,
            "spatial_pixels_visited": 4,
            "covered_pixels": 4,
            "expected_pixels": 4,
            "overlap_write_pixels": 0,
            "coverage_complete": True,
            "write_chunks": 2,
            "structure_value_domain": (
                "raw_paired_finite_stored_values_before_image_mask_application"
            ),
            "left_raw_finite_pixels": 4,
            "right_raw_finite_pixels": 4,
            "paired_raw_finite_pixels": 4,
            "paired_image_mask_finite_pixels": 4,
            "central_mask_mismatch_pixels": 0,
            "workspace_lifecycle": "remove_on_success_retain_on_failure",
        },
        "beam_info": copy.deepcopy(beam),
        "mask": {"type": "finite_overlap"},
        "masked_pixels": 4,
        "analysis_pixels": 4,
        "beam_block_side_pixels": 1,
        "normalization": {
            "type": "casa_support_rms_or_peak",
            "value": normalization,
        },
        "diff_rms": diff_rms,
        "normalized_diff_rms": normalized_diff_rms,
        "low_order_r2_quadratic": low_order_r2,
        "large_scale_power_fraction": large_scale_power,
        "scale_offset_gradient_fit": {"status": "insufficient_pixels"},
        "beam_block_rms_by_scale": [],
        "block_rms_decay_slope_vs_independent_beams": block_decay,
        "classification": classification,
        "review": review,
    }


def full(value: dict[str, object]) -> dict[str, object]:
    return value["products"][SUFFIX]["full_array"]  # type: ignore[index,return-value]


def topology(value: dict[str, object]) -> dict[str, object]:
    return full(value)["topology"]  # type: ignore[return-value]


def incomplete_coverage(value: dict[str, object]) -> None:
    full(value)["elements_visited"] = 3
    full(value)["coverage_complete"] = False


def forge_zero_difference_for_opposite_operands(value: dict[str, object]) -> None:
    """Claim zero difference for aggregates that prove opposite-valued operands."""

    product = value["products"][SUFFIX]  # type: ignore[index]
    evidence = product["full_array"]  # type: ignore[index]
    left_peak = peak([0, 0], 1.0)
    right_peak = peak([0, 0], -1.0)
    zero_peak = peak([0, 0], 0.0)
    evidence["left"] = operand(-1.0, 1.0, 0.0, 4.0, 1.0, left_peak)
    evidence["right"] = operand(-1.0, 1.0, 0.0, 4.0, 1.0, right_peak)
    evidence.update(
        {
            "cross_sum": -4.0,
            "covariance": -1.0,
            "correlation": -1.0,
            "left_integrated_value": 0.0,
            "right_integrated_value": 0.0,
            "diff_integrated_value": 0.0,
            "left_peak_abs": copy.deepcopy(left_peak),
            "right_peak_abs": copy.deepcopy(right_peak),
            "diff_peak_abs": copy.deepcopy(zero_peak),
            "difference": {
                "sum": 0.0,
                "sum_squares": 0.0,
                "integrated_value": 0.0,
                "rms": 0.0,
                "abs_max": 0.0,
                "peak_abs": copy.deepcopy(zero_peak),
            },
            "diff_rms": 0.0,
            "diff_abs_max": 0.0,
            "diff_rms_over_right_rms": 0.0,
            "diff_abs_max_over_right_peak": 0.0,
        }
    )
    product.update(
        {
            "left_min": -1.0,
            "left_max": 1.0,
            "left_rms": 1.0,
            "right_min": -1.0,
            "right_max": 1.0,
            "right_rms": 1.0,
            "left_peak_abs": copy.deepcopy(left_peak),
            "right_peak_abs": copy.deepcopy(right_peak),
            "diff_peak_abs": copy.deepcopy(zero_peak),
            "diff_abs_max": 0.0,
            "diff_rms": 0.0,
            "diff_rms_over_right_rms": 0.0,
            "diff_abs_max_over_right_peak": 0.0,
            "correlation": -1.0,
        }
    )


def forge_impossible_nonfinite_kind_parity(value: dict[str, object]) -> None:
    """Claim identical kinds when the left has NaN and the right has +Inf."""

    product = value["products"][SUFFIX]  # type: ignore[index]
    evidence = product["full_array"]  # type: ignore[index]
    evidence["count"] = 3
    evidence["comparison_domain_count"] = 3
    topology_evidence = evidence["topology"]
    topology_evidence.update(
        {
            "finite_equal": True,
            "finite_topology_mismatch_count": 0,
            "nonfinite_kind_equal": True,
            "nonfinite_kind_mismatch_count": 0,
            "left_finite_count": 3,
            "right_finite_count": 3,
            "left_nonfinite": {
                "nan": 1,
                "positive_infinity": 0,
                "negative_infinity": 0,
            },
            "right_nonfinite": {
                "nan": 0,
                "positive_infinity": 1,
                "negative_infinity": 0,
            },
        }
    )
    unit_peak = peak([0, 0], 1.0)
    zero_peak = peak([0, 0], 0.0)
    unit_operand = operand(1.0, 1.0, 3.0, 3.0, 1.0, unit_peak)
    evidence["left"] = copy.deepcopy(unit_operand)
    evidence["right"] = copy.deepcopy(unit_operand)
    evidence.update(
        {
            "cross_sum": 3.0,
            "covariance": 0.0,
            "correlation": None,
            "left_integrated_value": 3.0,
            "right_integrated_value": 3.0,
            "diff_integrated_value": 0.0,
            "left_peak_abs": copy.deepcopy(unit_peak),
            "right_peak_abs": copy.deepcopy(unit_peak),
            "diff_peak_abs": copy.deepcopy(zero_peak),
            "difference": {
                "sum": 0.0,
                "sum_squares": 0.0,
                "integrated_value": 0.0,
                "rms": 0.0,
                "abs_max": 0.0,
                "peak_abs": copy.deepcopy(zero_peak),
            },
            "diff_rms": 0.0,
            "diff_abs_max": 0.0,
            "diff_rms_over_right_rms": 0.0,
            "diff_abs_max_over_right_peak": 0.0,
        }
    )
    product.update(
        {
            "finite_overlap": 3,
            "topology_parity": True,
            "left_min": 1.0,
            "left_max": 1.0,
            "left_rms": 1.0,
            "right_min": 1.0,
            "right_max": 1.0,
            "right_rms": 1.0,
            "left_peak_abs": copy.deepcopy(unit_peak),
            "right_peak_abs": copy.deepcopy(unit_peak),
            "diff_peak_abs": copy.deepcopy(zero_peak),
            "diff_abs_max": 0.0,
            "diff_rms": 0.0,
            "diff_rms_over_right_rms": 0.0,
            "diff_abs_max_over_right_peak": 0.0,
            "correlation": None,
        }
    )


if __name__ == "__main__":
    unittest.main()
