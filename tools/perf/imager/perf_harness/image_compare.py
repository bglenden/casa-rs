# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical imaging-product comparison boundary."""

from __future__ import annotations

import hashlib
import json
import math
import pathlib
from typing import Any

from .casa_protocol import run_json_file_protocol
from .tolerances import (
    ToleranceContractError,
    evaluate_comparison_tolerances,
    validate_tolerance_contract,
)
from .tree_identity import sha256_file


CASA_IMAGE_COMPARATOR = pathlib.Path(__file__).with_name("casa_image_compare.py")
COMPARISON_SCHEMA_VERSION = 4
FULL_STRUCTURE_EVIDENCE_SCOPE = "full_native_central_spatial_plane_disk_backed"
FULL_STRUCTURE_EVIDENCE_FIELDS = {
    "method",
    "source_shape",
    "storage",
    "array_count",
    "temporary_bytes",
    "spatial_pixels_visited",
    "covered_pixels",
    "expected_pixels",
    "overlap_write_pixels",
    "coverage_complete",
    "write_chunks",
    "structure_value_domain",
    "left_raw_finite_pixels",
    "right_raw_finite_pixels",
    "paired_raw_finite_pixels",
    "paired_image_mask_finite_pixels",
    "central_mask_mismatch_pixels",
    "workspace_lifecycle",
}
FULL_STRUCTURE_METHOD = "exact_native_central_plane_disk_backed_memmap"
FULL_STRUCTURE_STORAGE = "temporary_disk_backed_native_arrays"
FULL_STRUCTURE_VALUE_DOMAIN = (
    "raw_paired_finite_stored_values_before_image_mask_application"
)
FULL_STRUCTURE_WORKSPACE_LIFECYCLE = "remove_on_success_retain_on_failure"
FULL_STRUCTURE_COMMON_FIELDS = {
    "status",
    "evidence_scope",
    "native_spatial_evidence",
    "beam_info",
    "normalization",
    "diff_rms",
    "normalized_diff_rms",
    "low_order_r2_quadratic",
    "large_scale_power_fraction",
    "scale_offset_gradient_fit",
    "beam_block_rms_by_scale",
    "block_rms_decay_slope_vs_independent_beams",
    "classification",
    "review",
}
FULL_STRUCTURE_COMPUTED_FIELDS = FULL_STRUCTURE_COMMON_FIELDS | {
    "mask",
    "masked_pixels",
    "analysis_pixels",
    "beam_block_side_pixels",
}
STRUCTURE_BLOCK_METRIC_FIELDS = {
    "beam_width_multiplier",
    "block_side_pixels",
    "approx_independent_beams_per_block",
    "n_blocks",
    "block_mean_rms",
    "normalized_block_mean_rms",
    "median_abs_block_mean",
    "mean_pixel_rms_in_blocks",
    "block_mean_rms_over_mean_pixel_rms",
    "max_block_robust_z",
}
FULL_ARRAY_FIELDS = {
    "status",
    "shape",
    "full_chunk_elements",
    "chunks",
    "max_chunk_elements_observed",
    "total_elements",
    "elements_visited",
    "coverage_complete",
    "comparison_domain",
    "count",
    "comparison_domain_count",
    "topology",
    "left",
    "right",
    "cross_sum",
    "covariance",
    "correlation",
    "left_integrated_value",
    "right_integrated_value",
    "diff_integrated_value",
    "left_peak_abs",
    "right_peak_abs",
    "diff_peak_abs",
    "difference",
    "diff_rms",
    "diff_abs_max",
    "diff_rms_over_right_rms",
    "diff_abs_max_over_right_peak",
    "structured_difference",
}
FULL_ARRAY_TOPOLOGY_FIELDS = {
    "mask_equal",
    "mask_mismatch_count",
    "left_masked_count",
    "right_masked_count",
    "finite_equal",
    "finite_topology_mismatch_count",
    "nonfinite_kind_equal",
    "nonfinite_kind_mismatch_count",
    "left_finite_count",
    "right_finite_count",
    "left_nonfinite",
    "right_nonfinite",
}
FULL_ARRAY_NONFINITE_FIELDS = {
    "nan",
    "positive_infinity",
    "negative_infinity",
}
FULL_ARRAY_OPERAND_FIELDS = {
    "min",
    "max",
    "sum",
    "sum_squares",
    "rms",
    "integrated_value",
    "peak_abs",
}
FULL_ARRAY_DIFFERENCE_FIELDS = {
    "sum",
    "sum_squares",
    "integrated_value",
    "rms",
    "abs_max",
    "peak_abs",
}
FULL_ARRAY_PEAK_FIELDS = {"location", "value", "abs_value"}
FULL_ARRAY_COMPARISON_DOMAIN = "left_and_right_pixel_masks_and_finite_values"
LEGACY_FULL_ARRAY_MIRRORS = {
    "rust_min": "left_min",
    "rust_max": "left_max",
    "rust_rms": "left_rms",
    "casa_min": "right_min",
    "casa_max": "right_max",
    "casa_rms": "right_rms",
    "diff_rms_over_casa_rms": "diff_rms_over_right_rms",
    "diff_abs_max_over_casa_peak": "diff_abs_max_over_right_peak",
    "rust_peak_abs": "left_peak_abs",
    "casa_peak_abs": "right_peak_abs",
}
COMPARISON_REQUEST_FIELDS = {
    "mode",
    "left_prefix",
    "right_prefix",
    "rust_prefix",
    "casa_prefix",
    "left_label",
    "right_label",
    "products",
    "max_elements_per_product",
    "full_chunk_elements",
    "require_exact_product_inventory",
    "require_metadata_parity",
    "source_regions",
    "tolerances",
    "panel_dir",
    "structure_workspace_dir",
}


def normalize_comparison_request(request: dict[str, Any]) -> dict[str, Any]:
    """Return the explicit, neutral image-comparison protocol request.

    ``rust_prefix`` and ``casa_prefix`` remain accepted for older workload
    manifests, but the protocol itself always receives neutral left/right
    operands.  This is important for CASA repeatability runs where neither
    operand is a Rust product.
    """

    if not isinstance(request, dict):
        raise ValueError("image comparison request must be an object")
    unknown = sorted(set(request) - COMPARISON_REQUEST_FIELDS)
    if unknown:
        raise ValueError(
            "image comparison request contains unknown field(s): " + ", ".join(unknown)
        )
    normalized = dict(request)
    mode = normalized.get("mode", "sampled")
    if mode not in {"sampled", "full"}:
        raise ValueError("image comparison mode must be 'sampled' or 'full'")

    if (
        "left_prefix" in normalized
        and "rust_prefix" in normalized
        and normalized["left_prefix"] != normalized["rust_prefix"]
    ):
        raise ValueError("left_prefix and rust_prefix aliases disagree")
    if (
        "right_prefix" in normalized
        and "casa_prefix" in normalized
        and normalized["right_prefix"] != normalized["casa_prefix"]
    ):
        raise ValueError("right_prefix and casa_prefix aliases disagree")
    left_prefix = normalized.get("left_prefix", normalized.get("rust_prefix"))
    right_prefix = normalized.get("right_prefix", normalized.get("casa_prefix"))
    if not isinstance(left_prefix, str) or not left_prefix:
        raise ValueError("image comparison requires left_prefix (or rust_prefix)")
    if not isinstance(right_prefix, str) or not right_prefix:
        raise ValueError("image comparison requires right_prefix (or casa_prefix)")

    raw_max_elements = normalized.get("max_elements_per_product", 1_000_000)
    if isinstance(raw_max_elements, bool) or not isinstance(raw_max_elements, int):
        raise ValueError("max_elements_per_product must be an integer")
    max_elements = raw_max_elements
    if max_elements < 1:
        raise ValueError("max_elements_per_product must be >= 1")
    raw_full_chunk_elements = normalized.get("full_chunk_elements", max_elements)
    if isinstance(raw_full_chunk_elements, bool) or not isinstance(
        raw_full_chunk_elements, int
    ):
        raise ValueError("full_chunk_elements must be an integer")
    full_chunk_elements = raw_full_chunk_elements
    if full_chunk_elements < 1:
        raise ValueError("full_chunk_elements must be >= 1")

    legacy_operands = (
        "left_prefix" not in normalized and "right_prefix" not in normalized
    )
    for key in ("require_exact_product_inventory", "require_metadata_parity"):
        if key in normalized and not isinstance(normalized[key], bool):
            raise ValueError(f"{key} must be a boolean")
    tolerances = normalized.get("tolerances")
    if tolerances is not None:
        try:
            validate_tolerance_contract(tolerances)
        except ToleranceContractError as error:
            raise ValueError(str(error)) from error
    products = _normalize_product_suffixes(normalized.get("products"))
    source_regions = _normalize_source_regions(
        normalized.get("source_regions", []), products
    )
    panel_dir = normalized.get("panel_dir")
    if not isinstance(panel_dir, str) or not panel_dir:
        raise ValueError("image comparison panel_dir must be a non-empty string")
    structure_workspace_dir = normalized.get("structure_workspace_dir")
    if not isinstance(structure_workspace_dir, str) or not structure_workspace_dir:
        raise ValueError(
            "image comparison structure_workspace_dir must be a non-empty string"
        )
    if not pathlib.Path(structure_workspace_dir).is_absolute():
        raise ValueError("image comparison structure_workspace_dir must be absolute")
    left_label = normalized.get("left_label", "casa-rs" if legacy_operands else "left")
    right_label = normalized.get("right_label", "CASA" if legacy_operands else "right")
    if not isinstance(left_label, str) or not left_label:
        raise ValueError("image comparison left_label must be a non-empty string")
    if not isinstance(right_label, str) or not right_label:
        raise ValueError("image comparison right_label must be a non-empty string")
    normalized = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "mode": mode,
        "left_prefix": left_prefix,
        "right_prefix": right_prefix,
        "left_label": left_label,
        "right_label": right_label,
        "products": products,
        "max_elements_per_product": max_elements,
        "full_chunk_elements": full_chunk_elements,
        "require_exact_product_inventory": bool(
            normalized.get("require_exact_product_inventory", False)
        ),
        "require_metadata_parity": bool(
            normalized.get("require_metadata_parity", False)
        ),
        "legacy_operand_aliases": legacy_operands,
        "source_regions": source_regions,
        "tolerances": tolerances,
        "panel_dir": panel_dir,
        "structure_workspace_dir": structure_workspace_dir,
    }
    binding = comparison_request_binding(normalized)
    normalized["request_binding"] = binding
    normalized["request_sha256"] = _canonical_sha256(binding)
    return normalized


def comparison_request_binding(request: dict[str, Any]) -> dict[str, Any]:
    """Return every normalized field that can affect comparison evidence."""

    fields = (
        "schema_version",
        "mode",
        "left_prefix",
        "right_prefix",
        "left_label",
        "right_label",
        "products",
        "max_elements_per_product",
        "full_chunk_elements",
        "require_exact_product_inventory",
        "require_metadata_parity",
        "legacy_operand_aliases",
        "source_regions",
        "tolerances",
        "panel_dir",
        "structure_workspace_dir",
    )
    missing = [field for field in fields if field not in request]
    if missing:
        raise ValueError(
            "normalized image comparison request is missing field(s): "
            + ", ".join(missing)
        )
    return {field: request[field] for field in fields}


def validate_comparison_output(
    comparison: dict[str, Any], request: dict[str, Any]
) -> None:
    """Fail closed unless CASA's output is bound to this exact request."""

    if not isinstance(comparison, dict):
        raise ValueError("image comparison output must be an object")
    expected_binding = comparison_request_binding(request)
    expected_digest = _canonical_sha256(expected_binding)
    if comparison.get("request_binding") != expected_binding:
        raise ValueError(
            "image comparison output request_binding does not match request"
        )
    if comparison.get("request_sha256") != expected_digest:
        raise ValueError(
            "image comparison output request_sha256 does not match request"
        )
    if comparison.get("status") not in {"completed", "comparison_failed"}:
        raise ValueError("image comparison output status does not match protocol")
    exact_fields = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "comparison_mode": request["mode"],
        "max_elements_per_product": request["max_elements_per_product"],
        "full_chunk_elements": request["full_chunk_elements"],
        "left_prefix": request["left_prefix"],
        "right_prefix": request["right_prefix"],
        "left_label": request["left_label"],
        "right_label": request["right_label"],
        "requested_products": request["products"],
        "require_exact_product_inventory": request["require_exact_product_inventory"],
        "require_metadata_parity": request["require_metadata_parity"],
        "legacy_operand_aliases": request["legacy_operand_aliases"],
        "source_regions": request["source_regions"],
        "tolerances": request["tolerances"],
        "panel_dir": request["panel_dir"],
        "structure_workspace_dir": request["structure_workspace_dir"],
    }
    for field, expected in exact_fields.items():
        if field not in comparison or comparison[field] != expected:
            raise ValueError(f"image comparison output {field} does not match request")

    products = comparison.get("products")
    if not isinstance(products, dict) or set(products) != set(request["products"]):
        raise ValueError("image comparison output product set does not match request")
    for suffix in request["products"]:
        product = products[suffix]
        if not isinstance(product, dict):
            raise ValueError(f"image comparison product {suffix} must be an object")
        expected_left = request["left_prefix"] + suffix
        expected_right = request["right_prefix"] + suffix
        if product.get("left_path") != expected_left:
            raise ValueError(
                f"image comparison product {suffix} left_path does not match request"
            )
        if product.get("right_path") != expected_right:
            raise ValueError(
                f"image comparison product {suffix} right_path does not match request"
            )
        if product.get("status") == "compared":
            _validate_product_metadata(
                product,
                suffix=suffix,
                required=request["require_metadata_parity"],
            )
            _validate_product_source_regions(
                product,
                suffix=suffix,
                requested_regions=[
                    region
                    for region in request["source_regions"]
                    if suffix in region["products"]
                ],
                requested_chunk_elements=request["full_chunk_elements"],
            )
        if request["mode"] == "full":
            _validate_full_array_evidence(
                product,
                suffix=suffix,
                requested_chunk_elements=request["full_chunk_elements"],
                legacy_operand_aliases=request["legacy_operand_aliases"],
            )
            _validate_full_structure_evidence(
                product,
                suffix=suffix,
                comparison_beam_info=comparison.get("beam_info"),
            )

    inventory = comparison.get("product_inventory")
    if not isinstance(inventory, dict):
        raise ValueError("image comparison output product_inventory must be an object")
    if inventory.get("expected") != sorted(set(request["products"])):
        raise ValueError(
            "image comparison inventory expected set does not match request"
        )
    if inventory.get("required") is not request["require_exact_product_inventory"]:
        raise ValueError("image comparison inventory policy does not match request")
    inventory_fields = {
        "status",
        "required",
        "observed_match",
        "expected",
        "left",
        "right",
        "left_missing",
        "left_extra",
        "right_missing",
        "right_extra",
        "left_right_equal",
    }
    if set(inventory) != inventory_fields:
        raise ValueError("image comparison inventory fields do not match protocol")
    left_inventory = inventory.get("left")
    right_inventory = inventory.get("right")
    if not all(
        isinstance(value, list)
        and value == sorted(value)
        and all(isinstance(item, str) and item.startswith(".") for item in value)
        for value in (left_inventory, right_inventory)
    ):
        raise ValueError("image comparison observed inventories are invalid")
    expected_products = sorted(set(request["products"]))
    left_missing = sorted(set(expected_products) - set(left_inventory))
    right_missing = sorted(set(expected_products) - set(right_inventory))
    left_extra = sorted(set(left_inventory) - set(expected_products))
    right_extra = sorted(set(right_inventory) - set(expected_products))
    mismatch = bool(left_missing or right_missing or left_extra or right_extra)
    if request["require_exact_product_inventory"] and mismatch:
        inventory_status = "mismatch"
    elif mismatch:
        inventory_status = "not_required"
    else:
        inventory_status = "matched"
    expected_inventory = {
        "status": inventory_status,
        "required": request["require_exact_product_inventory"],
        "observed_match": not mismatch,
        "expected": expected_products,
        "left": left_inventory,
        "right": right_inventory,
        "left_missing": left_missing,
        "left_extra": left_extra,
        "right_missing": right_missing,
        "right_extra": right_extra,
        "left_right_equal": left_inventory == right_inventory,
    }
    if inventory != expected_inventory:
        raise ValueError(
            "image comparison inventory derivation does not match protocol"
        )

    failures: list[str] = []
    if inventory["status"] == "mismatch":
        failures.append("exact product inventory differs")
    product_failures = [
        suffix
        for suffix in request["products"]
        if products[suffix].get("status") != "compared"
    ]
    if product_failures:
        failures.append("product comparison failed for " + ", ".join(product_failures))
    expected_status = "comparison_failed" if failures else "completed"
    expected_reason = "; ".join(failures) if failures else None
    if (
        comparison.get("status") != expected_status
        or comparison.get("reason") != expected_reason
    ):
        raise ValueError(
            "image comparison output status/reason is not derived from product "
            "and inventory results"
        )

    expected_review = _summarize_product_reviews(products)
    if comparison.get("structured_difference_review") != expected_review:
        raise ValueError(
            "image comparison structured_difference_review is not derived from "
            "product reviews"
        )


def _validate_product_metadata(
    product: dict[str, Any], *, suffix: str, required: bool
) -> None:
    label = f"image comparison product {suffix}"
    if product.get("metadata_parity_required") is not required:
        raise ValueError(f"{label} metadata parity policy does not match request")
    metadata = product.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError(f"{label} metadata result is missing")
    if not required:
        if metadata != {"status": "not_required", "parity": None}:
            raise ValueError(f"{label} unrequested metadata result is invalid")
        return

    expected_fields = {"status", "parity", "field_parity", "left", "right"}
    if set(metadata) != expected_fields:
        raise ValueError(f"{label} metadata result fields do not match protocol")
    if metadata.get("status") != "matched" or metadata.get("parity") is not True:
        raise ValueError(f"{label} required metadata parity is not matched")
    fields = ("shape", "unit", "coordinates", "restoring_beam", "masks")
    left = metadata.get("left")
    right = metadata.get("right")
    operand_fields = {
        "status",
        "shape",
        "unit",
        "coordinates",
        "restoring_beam",
        "masks",
        "errors",
    }
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise ValueError(f"{label} metadata operands must be objects")
    if set(left) != operand_fields or set(right) != operand_fields:
        raise ValueError(f"{label} metadata operand fields do not match protocol")
    for side, value in (("left", left), ("right", right)):
        if value.get("status") != "complete" or value.get("errors") != []:
            raise ValueError(f"{label} {side} metadata capture is incomplete")
        if value.get("shape") != product.get("shape"):
            raise ValueError(f"{label} {side} metadata shape does not match product")
    expected_parity = {field: left.get(field) == right.get(field) for field in fields}
    if metadata.get("field_parity") != expected_parity or not all(
        expected_parity.values()
    ):
        raise ValueError(f"{label} metadata parity is not derived from operands")


def _validate_product_source_regions(
    product: dict[str, Any],
    *,
    suffix: str,
    requested_regions: list[dict[str, Any]],
    requested_chunk_elements: int,
) -> None:
    label = f"image comparison product {suffix}"
    observed = product.get("source_regions")
    if not requested_regions:
        if observed is not None or "source_region_failure" in product:
            raise ValueError(f"{label} has unrequested source-region evidence")
        return
    if not isinstance(observed, list) or len(observed) != len(requested_regions):
        raise ValueError(f"{label} source-region inventory does not match request")
    for expected, region in zip(requested_regions, observed):
        if not isinstance(region, dict):
            raise ValueError(f"{label} source-region result must be an object")
        expected_fields = {"id", "products", "blc", "trc", "method", "left", "right"}
        if set(region) != expected_fields:
            raise ValueError(f"{label} source-region fields do not match protocol")
        for field in ("id", "products", "blc", "trc"):
            if region.get(field) != expected[field]:
                raise ValueError(
                    f"{label} source-region {field} is not bound to request"
                )
        if region.get("method") != (
            "finite_unmasked_region_sum_over_restoring_beam_area_"
            "and_abs_weighted_centroid"
        ):
            raise ValueError(f"{label} source-region method is invalid")
        for side in ("left", "right"):
            _validate_source_region_measurement(
                region.get(side),
                label=f"{label} source {expected['id']} {side}",
                blc=expected["blc"],
                trc=expected["trc"],
                requested_chunk_elements=requested_chunk_elements,
            )


def _validate_source_region_measurement(
    measurement: Any,
    *,
    label: str,
    blc: list[int],
    trc: list[int],
    requested_chunk_elements: int,
) -> None:
    expected_fields = {
        "status",
        "finite_unmasked_count",
        "integrated_pixel_sum",
        "beam_area_pixels",
        "integrated_flux",
        "centroid_pixels",
        "peak_abs",
        "chunks",
        "max_chunk_elements",
    }
    if not isinstance(measurement, dict) or set(measurement) != expected_fields:
        raise ValueError(f"{label} fields do not match protocol")
    if measurement.get("status") != "measured":
        raise ValueError(f"{label} did not complete")
    count = _nonnegative_integer(
        measurement.get("finite_unmasked_count"), label=f"{label} finite count"
    )
    region_pixels = math.prod(end - start + 1 for start, end in zip(blc, trc))
    if count < 1 or count > region_pixels:
        raise ValueError(f"{label} finite count is outside the source box")
    value_sum = _finite_number(
        measurement.get("integrated_pixel_sum"), label=f"{label} pixel sum"
    )
    beam_area = _finite_number(
        measurement.get("beam_area_pixels"), label=f"{label} beam area"
    )
    integrated_flux = _finite_number(
        measurement.get("integrated_flux"), label=f"{label} integrated flux"
    )
    if beam_area <= 0.0 or not _numbers_close(integrated_flux, value_sum / beam_area):
        raise ValueError(f"{label} integrated flux is not derived")
    centroid = measurement.get("centroid_pixels")
    if (
        not isinstance(centroid, list)
        or len(centroid) != 2
        or any(
            not _within_closed_interval(
                _finite_number(value, label=f"{label} centroid"), start, end
            )
            for value, start, end in zip(centroid, blc, trc)
        )
    ):
        raise ValueError(f"{label} centroid is outside the source box")
    peak = _validate_peak(
        measurement.get("peak_abs"),
        shape=[end + 1 for end in trc],
        label=f"{label} peak",
    )
    if any(
        location < start or location > end
        for location, start, end in zip(peak["location"], blc, trc)
    ):
        raise ValueError(f"{label} peak is outside the source box")
    chunks = _nonnegative_integer(measurement.get("chunks"), label=f"{label} chunks")
    chunk_elements = _nonnegative_integer(
        measurement.get("max_chunk_elements"), label=f"{label} chunk budget"
    )
    if chunks < 1 or chunk_elements != requested_chunk_elements:
        raise ValueError(f"{label} chunk evidence does not match request")


def _validate_full_array_evidence(
    product: dict[str, Any],
    *,
    suffix: str,
    requested_chunk_elements: int,
    legacy_operand_aliases: bool,
) -> None:
    """Rebind every passing full-mode scalar to its streamed evidence source."""

    if product.get("status") != "compared":
        return
    label = f"image comparison product {suffix}"
    full = product.get("full_array")
    if not isinstance(full, dict) or set(full) != FULL_ARRAY_FIELDS:
        raise ValueError(f"{label} full_array fields do not match protocol")
    if full.get("status") != "compared":
        raise ValueError(f"{label} full_array evidence is incomplete")

    shape = full.get("shape")
    if (
        not isinstance(shape, list)
        or not shape
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in shape
        )
    ):
        raise ValueError(f"{label} full_array shape is invalid")
    total_elements = math.prod(shape)
    recorded_total = _nonnegative_integer(
        full.get("total_elements"), label=f"{label} total_elements"
    )
    if recorded_total != total_elements:
        raise ValueError(f"{label} full_array total_elements is invalid")
    if product.get("shape") != shape:
        raise ValueError(f"{label} shape is not the authoritative full_array shape")

    chunk_budget = _nonnegative_integer(
        full.get("full_chunk_elements"), label=f"{label} full_chunk_elements"
    )
    chunks = _nonnegative_integer(full.get("chunks"), label=f"{label} chunks")
    max_chunk = _nonnegative_integer(
        full.get("max_chunk_elements_observed"),
        label=f"{label} max_chunk_elements_observed",
    )
    elements_visited = _nonnegative_integer(
        full.get("elements_visited"), label=f"{label} elements_visited"
    )
    if chunk_budget != requested_chunk_elements or chunk_budget < 1:
        raise ValueError(f"{label} full_array chunk budget does not match request")
    if (
        chunks < 1
        or max_chunk < 1
        or max_chunk > chunk_budget
        or max_chunk > elements_visited
        or chunks > elements_visited
        or chunks * max_chunk < elements_visited
        or chunks < math.ceil(elements_visited / chunk_budget)
    ):
        raise ValueError(f"{label} full_array chunk evidence is impossible")
    expected_coverage = elements_visited == total_elements
    if (
        not isinstance(full.get("coverage_complete"), bool)
        or full.get("coverage_complete") is not expected_coverage
    ):
        raise ValueError(f"{label} full_array coverage flag is not derived")
    if not expected_coverage:
        raise ValueError(f"{label} full_array coverage is incomplete")
    if full.get("comparison_domain") != FULL_ARRAY_COMPARISON_DOMAIN:
        raise ValueError(f"{label} full_array comparison domain is invalid")

    count = _nonnegative_integer(full.get("count"), label=f"{label} count")
    comparison_domain_count = _nonnegative_integer(
        full.get("comparison_domain_count"),
        label=f"{label} comparison_domain_count",
    )
    if count < 1 or count > total_elements or comparison_domain_count != count:
        raise ValueError(f"{label} full_array comparison count is invalid")

    topology_parity = _validate_full_array_topology(
        full.get("topology"), total_elements=total_elements, count=count, label=label
    )
    left = _validate_full_array_operand(
        full.get("left"), shape=shape, count=count, label=f"{label} left"
    )
    right = _validate_full_array_operand(
        full.get("right"), shape=shape, count=count, label=f"{label} right"
    )
    difference = _validate_full_array_difference(
        full.get("difference"), shape=shape, count=count, label=label
    )
    _validate_full_array_numerical_algebra(
        full,
        left=left,
        right=right,
        difference=difference,
        count=count,
        label=label,
    )
    _validate_full_array_product_mirrors(
        product,
        full=full,
        topology_parity=topology_parity,
        legacy_operand_aliases=legacy_operand_aliases,
        label=label,
    )


def _validate_full_array_topology(
    value: Any, *, total_elements: int, count: int, label: str
) -> bool:
    if not isinstance(value, dict) or set(value) != FULL_ARRAY_TOPOLOGY_FIELDS:
        raise ValueError(f"{label} full_array topology fields do not match protocol")
    count_fields = (
        "mask_mismatch_count",
        "left_masked_count",
        "right_masked_count",
        "finite_topology_mismatch_count",
        "nonfinite_kind_mismatch_count",
        "left_finite_count",
        "right_finite_count",
    )
    counts = {
        field: _nonnegative_integer(value.get(field), label=f"{label} {field}")
        for field in count_fields
    }
    if any(number > total_elements for number in counts.values()):
        raise ValueError(f"{label} full_array topology count exceeds total elements")

    flags_and_mismatches = (
        ("mask_equal", "mask_mismatch_count"),
        ("finite_equal", "finite_topology_mismatch_count"),
        ("nonfinite_kind_equal", "nonfinite_kind_mismatch_count"),
    )
    for flag, mismatch in flags_and_mismatches:
        if not isinstance(value.get(flag), bool) or value[flag] is not (
            counts[mismatch] == 0
        ):
            raise ValueError(f"{label} full_array topology flag {flag} is not derived")

    left_masked = counts["left_masked_count"]
    right_masked = counts["right_masked_count"]
    mask_mismatch = counts["mask_mismatch_count"]
    if (left_masked + right_masked - mask_mismatch) % 2:
        raise ValueError(f"{label} full_array mask topology has fractional regions")
    both_masked = (left_masked + right_masked - mask_mismatch) // 2
    left_only_masked = left_masked - both_masked
    right_only_masked = right_masked - both_masked
    both_unmasked = total_elements - (
        both_masked + left_only_masked + right_only_masked
    )
    if min(both_masked, left_only_masked, right_only_masked, both_unmasked) < 0:
        raise ValueError(f"{label} full_array mask topology counts are impossible")

    left_finite = counts["left_finite_count"]
    right_finite = counts["right_finite_count"]
    finite_mismatch = counts["finite_topology_mismatch_count"]
    if (
        left_finite > both_unmasked
        or right_finite > both_unmasked
        or finite_mismatch > both_unmasked
        or (left_finite + right_finite - finite_mismatch) % 2
    ):
        raise ValueError(f"{label} full_array finite topology counts are impossible")
    paired_finite = (left_finite + right_finite - finite_mismatch) // 2
    left_only_finite = left_finite - paired_finite
    right_only_finite = right_finite - paired_finite
    if min(paired_finite, left_only_finite, right_only_finite) < 0:
        raise ValueError(f"{label} full_array finite topology counts are impossible")
    if paired_finite != count:
        raise ValueError(
            f"{label} full_array comparison count is not derived from topology"
        )

    nonfinite_totals: dict[str, int] = {}
    categorical_counts: dict[str, dict[str, int]] = {}
    for side, finite in (("left", left_finite), ("right", right_finite)):
        nonfinite = value.get(f"{side}_nonfinite")
        if (
            not isinstance(nonfinite, dict)
            or set(nonfinite) != FULL_ARRAY_NONFINITE_FIELDS
        ):
            raise ValueError(
                f"{label} full_array {side} nonfinite fields do not match protocol"
            )
        side_counts = {
            field: _nonnegative_integer(
                nonfinite.get(field), label=f"{label} {side}_nonfinite.{field}"
            )
            for field in FULL_ARRAY_NONFINITE_FIELDS
        }
        categorical_counts[side] = {"finite": finite, **side_counts}
        nonfinite_totals[side] = sum(side_counts.values())
        if nonfinite_totals[side] != both_unmasked - finite:
            raise ValueError(
                f"{label} full_array {side} finite/nonfinite counts are not exhaustive"
            )
    recorded_kind_mismatch = counts["nonfinite_kind_mismatch_count"]
    kinds = ("finite", *sorted(FULL_ARRAY_NONFINITE_FIELDS))
    maximum_kind_matches = sum(
        min(categorical_counts["left"][kind], categorical_counts["right"][kind])
        for kind in kinds
    )
    minimum_kind_matches = sum(
        max(
            0,
            categorical_counts["left"][kind]
            + categorical_counts["right"][kind]
            - both_unmasked,
        )
        for kind in kinds
    )
    minimum_kind_mismatches = both_unmasked - maximum_kind_matches
    maximum_kind_mismatches = both_unmasked - minimum_kind_matches
    if not minimum_kind_mismatches <= recorded_kind_mismatch <= maximum_kind_mismatches:
        raise ValueError(
            f"{label} full_array nonfinite-kind mismatch count is inconsistent "
            "with categorical marginals"
        )
    if recorded_kind_mismatch < finite_mismatch:
        raise ValueError(
            f"{label} full_array nonfinite-kind mismatches omit finite mismatches"
        )
    if recorded_kind_mismatch > both_unmasked:
        raise ValueError(f"{label} full_array nonfinite-kind count is impossible")
    return bool(
        value["mask_equal"] and value["finite_equal"] and value["nonfinite_kind_equal"]
    )


def _validate_full_array_operand(
    value: Any, *, shape: list[int], count: int, label: str
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != FULL_ARRAY_OPERAND_FIELDS:
        raise ValueError(f"{label} fields do not match protocol")
    minimum = _finite_number(value.get("min"), label=f"{label}.min")
    maximum = _finite_number(value.get("max"), label=f"{label}.max")
    value_sum = _finite_number(value.get("sum"), label=f"{label}.sum")
    sum_squares = _finite_number(value.get("sum_squares"), label=f"{label}.sum_squares")
    rms = _finite_number(value.get("rms"), label=f"{label}.rms")
    integrated = _finite_number(
        value.get("integrated_value"), label=f"{label}.integrated_value"
    )
    if minimum > maximum or sum_squares < 0.0 or rms < 0.0:
        raise ValueError(f"{label} numerical bounds are invalid")
    if integrated != value_sum:
        raise ValueError(f"{label} integrated value is not its sum")
    expected_rms = math.sqrt(max(0.0, sum_squares / count))
    if not _numbers_close(rms, expected_rms):
        raise ValueError(f"{label} rms is not derived from sum_squares/count")
    mean = value_sum / count
    if not _within_closed_interval(mean, minimum, maximum):
        raise ValueError(f"{label} mean is outside min/max")
    if not _less_equal_with_roundoff(abs(value_sum), math.sqrt(count * sum_squares)):
        raise ValueError(f"{label} sum violates the sum-of-squares bound")
    peak = _validate_peak(value.get("peak_abs"), shape=shape, label=f"{label}.peak")
    expected_peak = max(abs(minimum), abs(maximum))
    if not _numbers_close(peak["abs_value"], expected_peak):
        raise ValueError(f"{label} peak does not match min/max")
    return value


def _validate_full_array_difference(
    value: Any, *, shape: list[int], count: int, label: str
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != FULL_ARRAY_DIFFERENCE_FIELDS:
        raise ValueError(f"{label} full_array difference fields do not match protocol")
    value_sum = _finite_number(value.get("sum"), label=f"{label} difference.sum")
    sum_squares = _finite_number(
        value.get("sum_squares"), label=f"{label} difference.sum_squares"
    )
    integrated = _finite_number(
        value.get("integrated_value"),
        label=f"{label} difference.integrated_value",
    )
    rms = _finite_number(value.get("rms"), label=f"{label} difference.rms")
    abs_max = _finite_number(value.get("abs_max"), label=f"{label} difference.abs_max")
    if sum_squares < 0.0 or rms < 0.0 or abs_max < 0.0:
        raise ValueError(f"{label} full_array difference bounds are invalid")
    if integrated != value_sum:
        raise ValueError(f"{label} difference integrated value is not its sum")
    if not _numbers_close(rms, math.sqrt(max(0.0, sum_squares / count))):
        raise ValueError(f"{label} difference rms is not derived")
    if not _less_equal_with_roundoff(rms, abs_max):
        raise ValueError(f"{label} difference rms exceeds abs_max")
    if not _less_equal_with_roundoff(abs(value_sum), count * abs_max):
        raise ValueError(f"{label} difference sum exceeds its abs_max bound")
    peak = _validate_peak(
        value.get("peak_abs"), shape=shape, label=f"{label} difference.peak"
    )
    if not _numbers_close(peak["abs_value"], abs_max):
        raise ValueError(f"{label} difference peak does not match abs_max")
    return value


def _validate_peak(value: Any, *, shape: list[int], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != FULL_ARRAY_PEAK_FIELDS:
        raise ValueError(f"{label} fields do not match protocol")
    location = value.get("location")
    if (
        not isinstance(location, list)
        or len(location) != len(shape)
        or any(
            isinstance(index, bool)
            or not isinstance(index, int)
            or index < 0
            or index >= size
            for index, size in zip(location, shape)
        )
    ):
        raise ValueError(f"{label} location is invalid")
    peak_value = _finite_number(value.get("value"), label=f"{label}.value")
    abs_value = _finite_number(value.get("abs_value"), label=f"{label}.abs_value")
    if abs_value < 0.0 or not _numbers_close(abs_value, abs(peak_value)):
        raise ValueError(f"{label} abs_value is not derived from value")
    return value


def _validate_full_array_numerical_algebra(
    full: dict[str, Any],
    *,
    left: dict[str, Any],
    right: dict[str, Any],
    difference: dict[str, Any],
    count: int,
    label: str,
) -> None:
    cross_sum = _finite_number(full.get("cross_sum"), label=f"{label} cross_sum")
    covariance = _finite_number(full.get("covariance"), label=f"{label} covariance")
    correlation = _optional_finite_number(
        full.get("correlation"), label=f"{label} correlation"
    )
    if correlation is not None and not _within_closed_interval(correlation, -1.0, 1.0):
        raise ValueError(f"{label} correlation is outside [-1, 1]")

    left_sum = float(left["sum"])
    right_sum = float(right["sum"])
    left_squares = float(left["sum_squares"])
    right_squares = float(right["sum_squares"])
    expected_covariance_numerator = cross_sum - left_sum * right_sum / count
    expected_covariance = expected_covariance_numerator / count
    if not _numbers_close(covariance, expected_covariance):
        raise ValueError(f"{label} covariance is not derived from streamed sums")
    left_variance_numerator = left_squares - left_sum * left_sum / count
    right_variance_numerator = right_squares - right_sum * right_sum / count
    denominator = math.sqrt(
        max(0.0, left_variance_numerator) * max(0.0, right_variance_numerator)
    )
    expected_correlation = (
        expected_covariance_numerator / denominator if denominator else None
    )
    if not _optional_numbers_close(correlation, expected_correlation):
        raise ValueError(f"{label} correlation is not derived from streamed sums")
    cross_bound = math.sqrt(left_squares * right_squares)
    if not _less_equal_with_roundoff(abs(cross_sum), cross_bound):
        raise ValueError(f"{label} cross_sum violates Cauchy-Schwarz")
    if not _numbers_close(
        float(difference["sum"]), left_sum - right_sum, scale=(left_sum, right_sum)
    ):
        raise ValueError(f"{label} difference sum is inconsistent with operands")
    expected_difference_sum_squares = left_squares + right_squares - 2.0 * cross_sum
    if not _numbers_close(
        float(difference["sum_squares"]),
        expected_difference_sum_squares,
        scale=(left_squares, right_squares, 2.0 * cross_sum),
    ):
        raise ValueError(
            f"{label} difference sum_squares is inconsistent with operands and cross_sum"
        )

    exact_mirrors = {
        "left_integrated_value": left["sum"],
        "right_integrated_value": right["sum"],
        "diff_integrated_value": difference["sum"],
        "left_peak_abs": left["peak_abs"],
        "right_peak_abs": right["peak_abs"],
        "diff_peak_abs": difference["peak_abs"],
        "diff_rms": difference["rms"],
        "diff_abs_max": difference["abs_max"],
        "correlation": correlation,
    }
    for field, expected in exact_mirrors.items():
        if full.get(field) != expected:
            raise ValueError(f"{label} full_array {field} mirror is inconsistent")

    expected_rms_ratio = _relative_ratio(float(difference["rms"]), float(right["rms"]))
    expected_peak_ratio = _relative_ratio(
        float(difference["abs_max"]), float(right["peak_abs"]["abs_value"])
    )
    if not _optional_numbers_close(
        _optional_finite_number(
            full.get("diff_rms_over_right_rms"),
            label=f"{label} diff_rms_over_right_rms",
        ),
        expected_rms_ratio,
    ):
        raise ValueError(f"{label} full_array rms ratio is not derived")
    if not _optional_numbers_close(
        _optional_finite_number(
            full.get("diff_abs_max_over_right_peak"),
            label=f"{label} diff_abs_max_over_right_peak",
        ),
        expected_peak_ratio,
    ):
        raise ValueError(f"{label} full_array peak ratio is not derived")


def _validate_full_array_product_mirrors(
    product: dict[str, Any],
    *,
    full: dict[str, Any],
    topology_parity: bool,
    legacy_operand_aliases: bool,
    label: str,
) -> None:
    mirrors = {
        "shape": full["shape"],
        "finite_overlap": full["comparison_domain_count"],
        "topology_parity": topology_parity,
        "left_min": full["left"]["min"],
        "left_max": full["left"]["max"],
        "left_rms": full["left"]["rms"],
        "right_min": full["right"]["min"],
        "right_max": full["right"]["max"],
        "right_rms": full["right"]["rms"],
        "left_peak_abs": full["left"]["peak_abs"],
        "right_peak_abs": full["right"]["peak_abs"],
        "diff_peak_abs": full["difference"]["peak_abs"],
        "diff_abs_max": full["diff_abs_max"],
        "diff_rms": full["diff_rms"],
        "diff_rms_over_right_rms": full["diff_rms_over_right_rms"],
        "diff_abs_max_over_right_peak": full["diff_abs_max_over_right_peak"],
        "correlation": full["correlation"],
    }
    for field, expected in mirrors.items():
        if field not in product or product[field] != expected:
            raise ValueError(
                f"{label} {field} is not the authoritative full_array value"
            )
    if not topology_parity:
        raise ValueError(f"{label} cannot be compared without topology parity")

    present_aliases = set(product) & set(LEGACY_FULL_ARRAY_MIRRORS)
    expected_aliases = (
        set(LEGACY_FULL_ARRAY_MIRRORS) if legacy_operand_aliases else set()
    )
    if present_aliases != expected_aliases:
        raise ValueError(f"{label} legacy full-array alias inventory is invalid")
    for alias, canonical in LEGACY_FULL_ARRAY_MIRRORS.items():
        if alias in product and product[alias] != product[canonical]:
            raise ValueError(f"{label} legacy alias {alias} is not derived")


def _nonnegative_integer(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _finite_number(value: Any, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{label} must be a finite number")
    return converted


def _optional_finite_number(value: Any, *, label: str) -> float | None:
    if value is None:
        return None
    return _finite_number(value, label=label)


def _numbers_close(left: float, right: float, *, scale: tuple[float, ...] = ()) -> bool:
    absolute_scale = max(1.0, abs(left), abs(right), *(abs(value) for value in scale))
    return math.isclose(
        float(left),
        float(right),
        rel_tol=2.0e-12,
        abs_tol=2.0e-12 * absolute_scale,
    )


def _optional_numbers_close(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is right
    return _numbers_close(left, right)


def _less_equal_with_roundoff(left: float, right: float) -> bool:
    return left <= right or _numbers_close(left, right)


def _within_closed_interval(value: float, lower: float, upper: float) -> bool:
    return _less_equal_with_roundoff(lower, value) and _less_equal_with_roundoff(
        value, upper
    )


def _relative_ratio(difference: float, reference: float) -> float | None:
    if reference != 0.0:
        return difference / abs(reference)
    return 0.0 if difference == 0.0 else None


def _validate_full_structure_evidence(
    product: dict[str, Any], *, suffix: str, comparison_beam_info: Any
) -> None:
    """Validate the exact, disk-backed full-mode structure evidence envelope."""

    if product.get("status") != "compared":
        return
    full_array = product.get("full_array")
    if not isinstance(full_array, dict) or full_array.get("status") != "compared":
        raise ValueError(
            f"image comparison product {suffix} full_array evidence is incomplete"
        )
    structure = product.get("structured_difference")
    if not isinstance(structure, dict):
        raise ValueError(
            f"image comparison product {suffix} structured_difference is missing"
        )
    if full_array.get("structured_difference") != structure:
        raise ValueError(
            f"image comparison product {suffix} structured_difference is not "
            "the authoritative full-array evidence"
        )
    if structure.get("evidence_scope") != FULL_STRUCTURE_EVIDENCE_SCOPE:
        raise ValueError(
            f"image comparison product {suffix} structure evidence scope is invalid"
        )
    evidence = structure.get("native_spatial_evidence")
    if (
        not isinstance(evidence, dict)
        or set(evidence) != FULL_STRUCTURE_EVIDENCE_FIELDS
    ):
        raise ValueError(
            f"image comparison product {suffix} native spatial evidence fields "
            "do not match protocol"
        )
    shape = evidence.get("source_shape")
    if (
        not isinstance(shape, list)
        or len(shape) != 2
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in shape
        )
    ):
        raise ValueError(
            f"image comparison product {suffix} native source_shape is invalid"
        )
    expected_pixels = shape[0] * shape[1]
    full_shape = full_array.get("shape")
    if (
        not isinstance(full_shape, list)
        or len(full_shape) < 2
        or full_shape[:2] != shape
    ):
        raise ValueError(
            f"image comparison product {suffix} native source_shape does not "
            "match the full array"
        )
    integer_fields = (
        "array_count",
        "temporary_bytes",
        "spatial_pixels_visited",
        "covered_pixels",
        "expected_pixels",
        "overlap_write_pixels",
        "write_chunks",
        "left_raw_finite_pixels",
        "right_raw_finite_pixels",
        "paired_raw_finite_pixels",
        "paired_image_mask_finite_pixels",
        "central_mask_mismatch_pixels",
    )
    if any(
        isinstance(evidence.get(field), bool)
        or not isinstance(evidence.get(field), int)
        or evidence[field] < 0
        for field in integer_fields
    ):
        raise ValueError(
            f"image comparison product {suffix} native spatial counts are invalid"
        )
    if evidence["expected_pixels"] != expected_pixels:
        raise ValueError(
            f"image comparison product {suffix} native expected_pixels is invalid"
        )
    exact_values = {
        "method": FULL_STRUCTURE_METHOD,
        "storage": FULL_STRUCTURE_STORAGE,
        "array_count": 4,
        "temporary_bytes": expected_pixels * 25,
        "structure_value_domain": FULL_STRUCTURE_VALUE_DOMAIN,
        "workspace_lifecycle": FULL_STRUCTURE_WORKSPACE_LIFECYCLE,
    }
    if any(evidence.get(field) != expected for field, expected in exact_values.items()):
        raise ValueError(
            f"image comparison product {suffix} native structure method/storage "
            "contract is invalid"
        )
    if evidence.get("coverage_complete") is not True:
        raise ValueError(
            f"image comparison product {suffix} native structure coverage is incomplete"
        )
    if (
        evidence["spatial_pixels_visited"] != expected_pixels
        or evidence["covered_pixels"] != expected_pixels
        or evidence["overlap_write_pixels"] != 0
        or evidence["write_chunks"] < 1
    ):
        raise ValueError(
            f"image comparison product {suffix} native structure coverage is invalid"
        )
    if (
        evidence["paired_raw_finite_pixels"]
        > min(evidence["left_raw_finite_pixels"], evidence["right_raw_finite_pixels"])
        or evidence["paired_image_mask_finite_pixels"]
        > evidence["paired_raw_finite_pixels"]
    ):
        raise ValueError(
            f"image comparison product {suffix} native finite counts are inconsistent"
        )
    for field in (
        "left_raw_finite_pixels",
        "right_raw_finite_pixels",
        "paired_raw_finite_pixels",
        "paired_image_mask_finite_pixels",
        "central_mask_mismatch_pixels",
    ):
        if evidence[field] > expected_pixels:
            raise ValueError(
                f"image comparison product {suffix} native spatial count exceeds "
                "the source plane"
            )
    structure_beam = structure.get("beam_info")
    if (
        not isinstance(comparison_beam_info, dict)
        or structure_beam != comparison_beam_info
    ):
        raise ValueError(
            f"image comparison product {suffix} native beam evidence does not "
            "match the comparison"
        )
    if comparison_beam_info.get("status") == "estimated_from_psf":
        if (
            comparison_beam_info.get("estimation_method")
            != "streamed_native_central_plane_peak_and_native_cross_sections"
            or comparison_beam_info.get("coordinate_domain")
            != "native_direction_pixels"
        ):
            raise ValueError(
                f"image comparison product {suffix} beam estimator is not native"
            )
        coverage = comparison_beam_info.get("native_plane_coverage")
        if (
            not isinstance(coverage, dict)
            or set(coverage)
            != {"pixels_visited", "expected_pixels", "coverage_complete"}
            or coverage.get("coverage_complete") is not True
            or isinstance(coverage.get("pixels_visited"), bool)
            or not isinstance(coverage.get("pixels_visited"), int)
            or isinstance(coverage.get("expected_pixels"), bool)
            or not isinstance(coverage.get("expected_pixels"), int)
            or coverage["pixels_visited"] != coverage["expected_pixels"]
            or coverage["expected_pixels"] < 1
        ):
            raise ValueError(
                f"image comparison product {suffix} native PSF coverage is invalid"
            )
    _validate_structure_semantics(product, structure=structure, suffix=suffix)


def _validate_structure_semantics(
    product: dict[str, Any], *, structure: dict[str, Any], suffix: str
) -> None:
    label = f"image comparison product {suffix}"
    status = structure.get("status")
    expected_fields = (
        FULL_STRUCTURE_COMMON_FIELDS
        if status == "not_applicable_exact_zero"
        else FULL_STRUCTURE_COMPUTED_FIELDS
    )
    if set(structure) != expected_fields:
        raise ValueError(f"{label} structure fields do not match protocol")
    classification = structure.get("classification")
    review = structure.get("review")
    if not isinstance(classification, dict) or not isinstance(review, dict):
        raise ValueError(f"{label} structure classification/review is missing")
    review_label = review.get("label")
    if status == "not_applicable_exact_zero":
        expected_label = "not_applicable_exact_zero"
        expected_classification = {
            "overall": expected_label,
            "amplitude": expected_label,
            "structure": expected_label,
            "structure_components": {},
            "thresholds": _structured_difference_thresholds(),
        }
        expected_review = {
            "label": expected_label,
            "summary": (
                f"{suffix}: structured difference is not applicable because full "
                "evidence proves both operands and their difference are exactly zero."
            ),
            "checks": [
                {
                    "name": "exact_zero_operands",
                    "label": expected_label,
                    "value": True,
                    "meaning": "full comparison domain proves both operands are zero",
                }
            ],
            "legend": _structured_difference_review_legend(),
        }
        if (
            classification != expected_classification
            or review != expected_review
            or structure.get("normalization")
            != {"type": "exact_zero_operands", "value": 0.0}
            or structure.get("diff_rms") != 0.0
            or structure.get("normalized_diff_rms") is not None
            or structure.get("low_order_r2_quadratic") is not None
            or structure.get("large_scale_power_fraction") is not None
            or structure.get("scale_offset_gradient_fit")
            != {
                "status": expected_label,
                "reason": "both operands and their difference are exactly zero",
            }
            or structure.get("beam_block_rms_by_scale") != []
            or structure.get("block_rms_decay_slope_vs_independent_beams") is not None
        ):
            raise ValueError(f"{label} exact-zero structure result is inconsistent")
        full = product["full_array"]
        zero_values = (
            full["left"]["min"],
            full["left"]["max"],
            full["left"]["sum_squares"],
            full["right"]["min"],
            full["right"]["max"],
            full["right"]["sum_squares"],
            full["difference"]["sum_squares"],
            full["difference"]["abs_max"],
        )
        if any(value != 0.0 for value in zero_values):
            raise ValueError(f"{label} exact-zero review contradicts full-array values")
        return
    if status != "computed":
        raise ValueError(f"{label} full structure result is incomplete")

    normalization = structure.get("normalization")
    if (
        not isinstance(normalization, dict)
        or set(normalization) != {"type", "value"}
        or normalization.get("type") != "casa_support_rms_or_peak"
    ):
        raise ValueError(f"{label} structure normalization is missing")
    normalization_value = _finite_number(
        normalization.get("value"), label=f"{label} structure normalization"
    )
    if normalization_value < 0.0:
        raise ValueError(f"{label} structure normalization is negative")
    diff_rms = _finite_number(
        structure.get("diff_rms"), label=f"{label} structure diff_rms"
    )
    if diff_rms < 0.0:
        raise ValueError(f"{label} structure diff_rms is negative")
    normalized = _optional_finite_number(
        structure.get("normalized_diff_rms"),
        label=f"{label} normalized structure difference",
    )
    expected_normalized = (
        diff_rms / normalization_value if normalization_value else None
    )
    if not _optional_numbers_close(normalized, expected_normalized):
        raise ValueError(f"{label} normalized structure difference is not derived")

    _validate_computed_structure_payload(
        structure,
        suffix=suffix,
        normalization_value=normalization_value,
    )

    amplitude = _classify_structure_amplitude(normalized)
    low_order = _optional_finite_number(
        structure.get("low_order_r2_quadratic"),
        label=f"{label} low-order structure score",
    )
    large_scale = structure.get("large_scale_power_fraction")
    large_scale_fraction = (
        _optional_finite_number(
            large_scale.get("fraction"),
            label=f"{label} large-scale power fraction",
        )
        if isinstance(large_scale, dict)
        else None
    )
    block_decay = _optional_finite_number(
        structure.get("block_rms_decay_slope_vs_independent_beams"),
        label=f"{label} block RMS decay slope",
    )
    if _non_spatial_product(suffix):
        expected_classification = {
            "overall": amplitude,
            "amplitude": amplitude,
            "structure": "not_applicable",
            "structure_components": {},
            "thresholds": _structured_difference_thresholds(),
        }
    else:
        components = {
            "block_rms_decay_slope_vs_independent_beams": (
                _classify_structure_block_decay(block_decay)
            ),
            "large_scale_power_fraction": _classify_structure_large_scale(
                large_scale_fraction
            ),
            "low_order_r2_quadratic": _classify_structure_low_order(low_order),
        }
        floor = normalized is not None and normalized < 1.0e-6
        if floor and amplitude == "good":
            structure_label = "good"
            overall = "good"
        else:
            structure_label = _worst_structure_label(components.values())
            overall = _overall_structure_label(amplitude, structure_label)
        expected_classification = {
            "overall": overall,
            "amplitude": amplitude,
            "structure": structure_label,
            "structure_components": components,
            "structure_suppressed_by_numerical_floor": floor,
            "thresholds": _structured_difference_thresholds(),
        }
    if classification != expected_classification:
        raise ValueError(f"{label} structure classification is not derived")
    expected_review = _structured_difference_review(
        suffix=suffix,
        classification=expected_classification,
        normalized_diff_rms=normalized,
        low_order_r2=low_order,
        large_scale_fraction=large_scale_fraction,
        block_decay_slope=block_decay,
    )
    if review != expected_review or review_label != expected_classification["overall"]:
        raise ValueError(f"{label} structure review is not derived from classification")


def _validate_computed_structure_payload(
    structure: dict[str, Any], *, suffix: str, normalization_value: float
) -> None:
    label = f"image comparison product {suffix}"
    evidence = structure["native_spatial_evidence"]
    expected_pixels = evidence["expected_pixels"]
    masked_pixels = _nonnegative_integer(
        structure.get("masked_pixels"), label=f"{label} structure masked_pixels"
    )
    analysis_pixels = _nonnegative_integer(
        structure.get("analysis_pixels"), label=f"{label} structure analysis_pixels"
    )
    if (
        masked_pixels < 1
        or analysis_pixels < 1
        or analysis_pixels > masked_pixels
        or masked_pixels > evidence["paired_raw_finite_pixels"]
        or masked_pixels > expected_pixels
    ):
        raise ValueError(f"{label} structure pixel counts are inconsistent")

    beam_side = _nonnegative_integer(
        structure.get("beam_block_side_pixels"),
        label=f"{label} structure beam block side",
    )
    if beam_side < 1:
        raise ValueError(f"{label} structure beam block side must be positive")
    beam_info = structure["beam_info"]
    expected_beam_side = 1
    if isinstance(beam_info, dict) and beam_info.get("status") == "estimated_from_psf":
        raw_beam_side = beam_info.get("beam_block_side_pixels") or 1
        expected_beam_side = _nonnegative_integer(
            raw_beam_side, label=f"{label} comparison beam block side"
        )
    if beam_side != max(1, expected_beam_side):
        raise ValueError(f"{label} structure beam block side is not derived")

    _validate_structure_mask(structure.get("mask"), suffix=suffix, label=label)
    _validate_structure_basis_fit(
        structure.get("scale_offset_gradient_fit"),
        suffix=suffix,
        source_shape=evidence["source_shape"],
        analysis_pixels=analysis_pixels,
        label=label,
    )
    _validate_large_scale_power(
        structure.get("large_scale_power_fraction"),
        suffix=suffix,
        beam_side=beam_side,
        label=label,
    )
    _validate_structure_block_metrics(
        structure.get("beam_block_rms_by_scale"),
        reported_slope=structure.get("block_rms_decay_slope_vs_independent_beams"),
        source_shape=evidence["source_shape"],
        suffix=suffix,
        beam_side=beam_side,
        normalization_value=normalization_value,
        label=label,
    )

    low_order = _optional_finite_number(
        structure.get("low_order_r2_quadratic"),
        label=f"{label} low-order structure score",
    )
    if low_order is not None and not _within_closed_interval(low_order, 0.0, 1.0):
        raise ValueError(f"{label} low-order structure score is outside [0, 1]")


def _validate_structure_mask(mask: Any, *, suffix: str, label: str) -> None:
    if not isinstance(mask, dict) or not isinstance(mask.get("type"), str):
        raise ValueError(f"{label} structure mask is invalid")
    mask_type = mask["type"]
    if mask_type == "finite_overlap":
        valid = set(mask) == {"type"}
    elif mask_type == "full_finite_overlap":
        valid = set(mask) == {"type", "product_family"} and mask.get(
            "product_family"
        ) == _product_family(suffix)
    elif mask_type == "weight_union_support":
        threshold = _finite_number(
            mask.get("threshold"), label=f"{label} weight mask threshold"
        )
        valid = (
            suffix == ".weight"
            and set(mask) == {"type", "threshold_fraction_of_peak", "threshold"}
            and mask.get("threshold_fraction_of_peak") == 1.0e-3
            and threshold > 0.0
        )
    elif mask_type == "casa_pb_support":
        valid = (
            suffix == ".pb"
            and set(mask) == {"type", "threshold"}
            and mask.get("threshold") == 0.01
        )
    else:
        valid = False
    if not valid:
        raise ValueError(f"{label} structure mask fields do not match protocol")


def _validate_structure_basis_fit(
    fit: Any,
    *,
    suffix: str,
    source_shape: list[int],
    analysis_pixels: int,
    label: str,
) -> None:
    if not isinstance(fit, dict) or not isinstance(fit.get("status"), str):
        raise ValueError(f"{label} scale/offset/gradient fit is invalid")
    if _non_spatial_product(suffix):
        if fit != {"status": "not_applicable", "reason": "non_spatial_product"}:
            raise ValueError(f"{label} non-spatial basis fit is invalid")
        return

    status = fit["status"]
    if status == "insufficient_pixels":
        if fit != {"status": status} or analysis_pixels >= 8:
            raise ValueError(f"{label} insufficient-pixel basis fit is inconsistent")
        return
    if status == "insufficient_dimensions":
        if (
            set(fit) != {"status", "shape"}
            or fit.get("shape") != source_shape
            or analysis_pixels < 8
            or min(source_shape) >= 2
        ):
            raise ValueError(
                f"{label} insufficient-dimension basis fit is inconsistent"
            )
        return
    if status == "insufficient_finite_basis_pixels":
        if set(fit) != {
            "status",
            "masked_pixels",
            "fit_pixels",
            "excluded_nonfinite_basis_pixels",
        }:
            raise ValueError(f"{label} finite-basis fit fields do not match protocol")
        masked = _nonnegative_integer(
            fit.get("masked_pixels"), label=f"{label} basis-fit masked pixels"
        )
        fitted = _nonnegative_integer(
            fit.get("fit_pixels"), label=f"{label} basis-fit pixels"
        )
        excluded = _nonnegative_integer(
            fit.get("excluded_nonfinite_basis_pixels"),
            label=f"{label} excluded basis-fit pixels",
        )
        if (
            masked != analysis_pixels
            or masked < 8
            or fitted >= 8
            or excluded != masked - fitted
        ):
            raise ValueError(f"{label} finite-basis fit counts are inconsistent")
        return
    if status != "computed" or set(fit) != {
        "status",
        "model",
        "masked_pixels",
        "fit_pixels",
        "excluded_nonfinite_basis_pixels",
        "r2",
        "diff_rms",
        "residual_rms",
        "coefficients",
    }:
        raise ValueError(f"{label} computed basis-fit fields do not match protocol")
    if fit.get("model") != (
        "diff ~= scale*reference + offset + dx*d_reference_dx + dy*d_reference_dy"
    ):
        raise ValueError(f"{label} basis-fit model is invalid")
    masked = _nonnegative_integer(
        fit.get("masked_pixels"), label=f"{label} basis-fit masked pixels"
    )
    fitted = _nonnegative_integer(
        fit.get("fit_pixels"), label=f"{label} basis-fit pixels"
    )
    excluded = _nonnegative_integer(
        fit.get("excluded_nonfinite_basis_pixels"),
        label=f"{label} excluded basis-fit pixels",
    )
    if (
        masked != analysis_pixels
        or fitted < 8
        or fitted > masked
        or excluded != masked - fitted
    ):
        raise ValueError(f"{label} basis-fit counts are inconsistent")
    coefficients = fit.get("coefficients")
    if not isinstance(coefficients, dict) or set(coefficients) != {
        "scale",
        "offset",
        "dx_pixels",
        "dy_pixels",
    }:
        raise ValueError(f"{label} basis-fit coefficients are invalid")
    for name, value in coefficients.items():
        _finite_number(value, label=f"{label} basis-fit coefficient {name}")
    diff_rms = _finite_number(fit.get("diff_rms"), label=f"{label} basis-fit RMS")
    residual_rms = _finite_number(
        fit.get("residual_rms"), label=f"{label} basis-fit residual RMS"
    )
    r2 = _optional_finite_number(fit.get("r2"), label=f"{label} basis-fit r2")
    if (
        diff_rms < 0.0
        or residual_rms < 0.0
        or not _less_equal_with_roundoff(residual_rms, diff_rms)
    ):
        raise ValueError(f"{label} basis-fit RMS values are inconsistent")
    expected_r2 = (
        1.0 - (residual_rms * residual_rms) / (diff_rms * diff_rms)
        if diff_rms > 0.0
        else None
    )
    if not _optional_numbers_close(r2, expected_r2):
        raise ValueError(f"{label} basis-fit r2 is not derived")


def _validate_large_scale_power(
    power: Any, *, suffix: str, beam_side: int, label: str
) -> None:
    if _non_spatial_product(suffix):
        if power is not None:
            raise ValueError(f"{label} non-spatial large-scale power is invalid")
        return
    if power is None:
        return
    if not isinstance(power, dict) or set(power) != {
        "min_wavelength_beams",
        "frequency_cutoff_cycles_per_pixel",
        "fraction",
    }:
        raise ValueError(f"{label} large-scale power fields do not match protocol")
    wavelength = _finite_number(
        power.get("min_wavelength_beams"),
        label=f"{label} large-scale minimum wavelength",
    )
    cutoff = _finite_number(
        power.get("frequency_cutoff_cycles_per_pixel"),
        label=f"{label} large-scale frequency cutoff",
    )
    fraction = _finite_number(
        power.get("fraction"), label=f"{label} large-scale power fraction"
    )
    expected_cutoff = 1.0 / max(1.0, float(beam_side) * 8.0)
    if (
        wavelength != 8.0
        or not _numbers_close(cutoff, expected_cutoff)
        or not _within_closed_interval(fraction, 0.0, 1.0)
    ):
        raise ValueError(f"{label} large-scale power is inconsistent")


def _validate_structure_block_metrics(
    scales: Any,
    *,
    reported_slope: Any,
    source_shape: list[int],
    suffix: str,
    beam_side: int,
    normalization_value: float,
    label: str,
) -> None:
    slope = _optional_finite_number(
        reported_slope, label=f"{label} block RMS decay slope"
    )
    if _non_spatial_product(suffix):
        if scales != [] or slope is not None:
            raise ValueError(f"{label} non-spatial block metrics are invalid")
        return
    if not isinstance(scales, list) or len(scales) > 6:
        raise ValueError(f"{label} block metric inventory is invalid")
    allowed_multipliers = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0]
    observed_multipliers: list[float] = []
    regression_points: list[tuple[float, float]] = []
    height, width = source_shape
    for index, metric in enumerate(scales):
        metric_label = f"{label} block metric {index}"
        if not isinstance(metric, dict) or set(metric) != STRUCTURE_BLOCK_METRIC_FIELDS:
            raise ValueError(f"{metric_label} fields do not match protocol")
        multiplier = _finite_number(
            metric.get("beam_width_multiplier"),
            label=f"{metric_label} beam multiplier",
        )
        if multiplier not in allowed_multipliers:
            raise ValueError(f"{metric_label} beam multiplier is invalid")
        observed_multipliers.append(multiplier)
        side = _nonnegative_integer(
            metric.get("block_side_pixels"), label=f"{metric_label} block side"
        )
        expected_side = max(1, int(round(float(beam_side) * multiplier)))
        if side != expected_side:
            raise ValueError(f"{metric_label} block side is not derived")
        independent_beams = _finite_number(
            metric.get("approx_independent_beams_per_block"),
            label=f"{metric_label} independent beams",
        )
        expected_beams = (float(side) / max(1.0, float(beam_side))) ** 2
        if not _numbers_close(independent_beams, expected_beams):
            raise ValueError(f"{metric_label} independent-beam count is not derived")
        blocks = _nonnegative_integer(
            metric.get("n_blocks"), label=f"{metric_label} block count"
        )
        maximum_blocks = math.ceil(height / side) * math.ceil(width / side)
        if blocks < 3 or blocks > maximum_blocks:
            raise ValueError(f"{metric_label} block count is inconsistent")
        block_rms = _finite_number(
            metric.get("block_mean_rms"), label=f"{metric_label} block RMS"
        )
        median_abs = _finite_number(
            metric.get("median_abs_block_mean"),
            label=f"{metric_label} median absolute block mean",
        )
        pixel_rms = _finite_number(
            metric.get("mean_pixel_rms_in_blocks"),
            label=f"{metric_label} mean pixel RMS",
        )
        normalized = _optional_finite_number(
            metric.get("normalized_block_mean_rms"),
            label=f"{metric_label} normalized block RMS",
        )
        pixel_ratio = _optional_finite_number(
            metric.get("block_mean_rms_over_mean_pixel_rms"),
            label=f"{metric_label} block/pixel RMS ratio",
        )
        robust_z = _optional_finite_number(
            metric.get("max_block_robust_z"),
            label=f"{metric_label} maximum robust z",
        )
        if any(value < 0.0 for value in (block_rms, median_abs, pixel_rms)) or (
            robust_z is not None and robust_z < 0.0
        ):
            raise ValueError(f"{metric_label} contains a negative scale metric")
        expected_normalized = (
            block_rms / normalization_value if normalization_value else None
        )
        expected_pixel_ratio = block_rms / pixel_rms if pixel_rms else None
        if not _optional_numbers_close(normalized, expected_normalized):
            raise ValueError(f"{metric_label} normalized RMS is not derived")
        if not _optional_numbers_close(pixel_ratio, expected_pixel_ratio):
            raise ValueError(f"{metric_label} block/pixel RMS ratio is not derived")
        if normalized is not None and normalized > 0.0 and independent_beams > 0.0:
            regression_points.append(
                (math.log(independent_beams), math.log(normalized))
            )
    if observed_multipliers != sorted(set(observed_multipliers)):
        raise ValueError(f"{label} block metric scales are not ordered and unique")
    expected_slope = _least_squares_slope(regression_points)
    if not _optional_numbers_close(slope, expected_slope):
        raise ValueError(f"{label} block RMS decay slope is not derived")


def _least_squares_slope(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 2:
        return None
    mean_x = math.fsum(point[0] for point in points) / len(points)
    mean_y = math.fsum(point[1] for point in points) / len(points)
    denominator = math.fsum((point[0] - mean_x) ** 2 for point in points)
    if denominator == 0.0:
        return None
    return (
        math.fsum((point[0] - mean_x) * (point[1] - mean_y) for point in points)
        / denominator
    )


def _product_family(suffix: str) -> str:
    for family in (".image", ".residual", ".model", ".psf", ".sumwt", ".pb", ".weight"):
        if suffix == family or suffix.startswith(family + ".tt"):
            return family
    return suffix


def _non_spatial_product(suffix: str) -> bool:
    return suffix == ".sumwt" or suffix.startswith(".sumwt.tt")


def _classify_structure_amplitude(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 1.0e-4:
        return "good"
    if value <= 1.0e-3:
        return "investigate"
    return "bad"


def _classify_structure_block_decay(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= -0.35:
        return "good"
    if value <= -0.15:
        return "investigate"
    return "bad"


def _classify_structure_large_scale(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.25:
        return "good"
    if value <= 0.5:
        return "investigate"
    return "bad"


def _classify_structure_low_order(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.05:
        return "good"
    if value <= 0.2:
        return "investigate"
    return "bad"


def _worst_structure_label(values: Any) -> str:
    rank = {"unknown": 0, "good": 1, "investigate": 2, "bad": 3}
    labels = list(values)
    return max(labels, key=lambda value: rank.get(value, 0)) if labels else "unknown"


def _overall_structure_label(amplitude: str, structure: str) -> str:
    if amplitude == "bad" or (amplitude == "investigate" and structure == "bad"):
        return "bad"
    if amplitude == "good" and structure == "good":
        return "good"
    if amplitude == "unknown" and structure == "unknown":
        return "unknown"
    return "investigate"


def _structured_difference_thresholds() -> dict[str, dict[str, str]]:
    return {
        "normalized_diff_rms": {
            "good": "< 1e-4",
            "numerical_floor": "< 1e-6 suppresses structure-only escalation",
            "investigate": "1e-4 .. 1e-3",
            "bad": "> 1e-3",
        },
        "block_rms_decay_slope_vs_independent_beams": {
            "good": "<= -0.35",
            "investigate": "-0.35 .. -0.15",
            "bad": "> -0.15",
        },
        "large_scale_power_fraction": {
            "good": "< 0.25",
            "investigate": "0.25 .. 0.5",
            "bad": "> 0.5",
        },
        "low_order_r2_quadratic": {
            "good": "< 0.05",
            "investigate": "0.05 .. 0.2",
            "bad": "> 0.2",
        },
    }


def _structured_difference_review(
    *,
    suffix: str,
    classification: dict[str, Any],
    normalized_diff_rms: float | None,
    low_order_r2: float | None,
    large_scale_fraction: float | None,
    block_decay_slope: float | None,
) -> dict[str, Any]:
    components = classification.get("structure_components", {})
    return {
        "label": classification.get("overall", "unknown"),
        "summary": _structured_difference_review_summary(suffix, classification),
        "checks": [
            {
                "name": "normalized_diff_rms",
                "label": classification.get("amplitude", "unknown"),
                "value": normalized_diff_rms,
                "meaning": "beam/product-scale RMS amplitude difference",
            },
            {
                "name": "block_rms_decay_slope_vs_independent_beams",
                "label": components.get(
                    "block_rms_decay_slope_vs_independent_beams", "unknown"
                ),
                "value": block_decay_slope,
                "meaning": (
                    "whether averaging over independent beams suppresses the difference"
                ),
            },
            {
                "name": "large_scale_power_fraction",
                "label": components.get("large_scale_power_fraction", "unknown"),
                "value": large_scale_fraction,
                "meaning": (
                    "fraction of difference power on scales much larger than the beam"
                ),
            },
            {
                "name": "low_order_r2_quadratic",
                "label": components.get("low_order_r2_quadratic", "unknown"),
                "value": low_order_r2,
                "meaning": (
                    "fraction of difference variance explained by a smooth quadratic "
                    "surface"
                ),
            },
        ],
        "legend": _structured_difference_review_legend(),
    }


def _structured_difference_review_summary(
    suffix: str, classification: dict[str, Any]
) -> str:
    overall = classification.get("overall", "unknown")
    amplitude = classification.get("amplitude", "unknown")
    structure = classification.get("structure", "unknown")
    if structure == "not_applicable":
        if overall == "good":
            return f"{suffix}: good; non-spatial product amplitude check passed."
        if overall == "bad":
            return (
                f"{suffix}: bad; non-spatial product amplitude is {amplitude}. "
                "Treat this as a correctness blocker until instrumented or explained."
            )
        if overall == "investigate":
            return (
                f"{suffix}: investigate; non-spatial product amplitude is {amplitude}."
            )
        return f"{suffix}: unknown; non-spatial product amplitude check did not run."
    if overall == "good":
        return f"{suffix}: good; amplitude and beam-scale structure checks passed."
    if overall == "bad":
        return (
            f"{suffix}: bad; amplitude is {amplitude} and structure is {structure}. "
            "Treat this as a correctness blocker until instrumented or explained."
        )
    if overall == "investigate":
        return (
            f"{suffix}: investigate; amplitude is {amplitude} and structure is "
            f"{structure}."
        )
    return f"{suffix}: unknown; one or more structured-difference checks did not run."


def _structured_difference_review_legend() -> dict[str, str]:
    return {
        "good": "No review action expected from this check.",
        "investigate": "Plausible but needs review in context.",
        "bad": (
            "Structured or large enough difference; do not close without explanation."
        ),
        "unknown": "Check could not be evaluated for this product.",
        "not_applicable_exact_zero": (
            "Full evidence proves both operands and their difference are exactly zero."
        ),
    }


def _summarize_product_reviews(products: dict[str, Any]) -> dict[str, Any]:
    product_labels: dict[str, Any] = {}
    product_summaries: dict[str, Any] = {}
    check_labels: dict[str, dict[str, Any]] = {}
    for suffix, product in sorted(products.items()):
        if not isinstance(product, dict):
            continue
        structure = product.get("structured_difference")
        if not isinstance(structure, dict):
            product_labels[suffix] = product.get("status", "unknown")
            continue
        review = structure.get("review")
        if not isinstance(review, dict):
            product_labels[suffix] = structure.get("status", "unknown")
            continue
        product_labels[suffix] = review.get("label", "unknown")
        product_summaries[suffix] = review.get("summary")
        checks = review.get("checks", [])
        if not isinstance(checks, list):
            continue
        for check in checks:
            if not isinstance(check, dict):
                continue
            name = check.get("name")
            if isinstance(name, str):
                check_labels.setdefault(name, {})[suffix] = check.get(
                    "label", "unknown"
                )
    overall = _worst_review_label(product_labels.values())
    return {
        "label": overall,
        "summary": _structured_difference_rollup_summary(overall, product_labels),
        "products": product_labels,
        "product_summaries": product_summaries,
        "checks_by_product": check_labels,
        "thresholds": _structured_difference_thresholds(),
        "legend": _structured_difference_review_legend(),
    }


def _worst_review_label(labels: Any) -> str:
    rank = {
        "not_applicable_exact_zero": 0,
        "good": 1,
        "investigate": 2,
        "unknown": 3,
        "bad": 4,
    }
    values = list(labels)
    if not values:
        return "unknown"
    return max(values, key=lambda label: rank.get(label, 0))


def _structured_difference_rollup_summary(
    overall: str, product_labels: dict[str, Any]
) -> str:
    if not product_labels:
        return "No structured-difference product reviews were available."
    order = (
        "bad",
        "unknown",
        "investigate",
        "good",
        "not_applicable_exact_zero",
    )
    parts = []
    for label in order:
        suffixes = [
            suffix
            for suffix, product_label in product_labels.items()
            if product_label == label
        ]
        if suffixes:
            parts.append(f"{label}: {', '.join(suffixes)}")
    return f"overall {overall}; " + "; ".join(parts)


def _normalize_product_suffixes(value: Any) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item.startswith(".") for item in value)
    ):
        raise ValueError("image comparison products must be a non-empty suffix list")
    if len(set(value)) != len(value):
        raise ValueError("image comparison product suffixes must be unique")
    return list(value)


def _normalize_source_regions(
    regions: Any, products: list[str]
) -> list[dict[str, Any]]:
    if not isinstance(regions, list):
        raise ValueError("image comparison source_regions must be a list")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for region in regions:
        if not isinstance(region, dict) or set(region) != {
            "id",
            "products",
            "blc",
            "trc",
        }:
            raise ValueError("source region fields must be id, products, blc, and trc")
        region_id = region.get("id")
        if not isinstance(region_id, str) or not region_id or region_id in seen:
            raise ValueError("source region ids must be nonempty and unique")
        seen.add(region_id)
        suffixes = region.get("products")
        if (
            not isinstance(suffixes, list)
            or not suffixes
            or len(set(suffixes)) != len(suffixes)
            or not all(isinstance(item, str) and item in products for item in suffixes)
        ):
            raise ValueError(
                "source region products must name unique compared suffixes"
            )
        blc = region.get("blc")
        trc = region.get("trc")
        if (
            not isinstance(blc, list)
            or not isinstance(trc, list)
            or len(blc) != 2
            or len(trc) != 2
            or not all(
                isinstance(item, int) and not isinstance(item, bool)
                for item in [*blc, *trc]
            )
            or any(start < 0 or end < start for start, end in zip(blc, trc))
        ):
            raise ValueError(
                "source region blc/trc must be ordered nonnegative [x, y] pixels"
            )
        normalized.append(
            {
                "id": region_id,
                "products": list(suffixes),
                "blc": list(blc),
                "trc": list(trc),
            }
        )
    return normalized


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compare_products(
    *,
    casa_python: str | None,
    request: dict[str, Any],
    artifact_prefix: pathlib.Path,
    cwd: pathlib.Path,
) -> dict[str, Any]:
    request = normalize_comparison_request(request)
    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=CASA_IMAGE_COMPARATOR,
        request=request,
        request_path=artifact_prefix.with_suffix(".comparison-input.json"),
        output_path=artifact_prefix.with_suffix(".comparison.json"),
        log_path=artifact_prefix.with_suffix(".comparison.log"),
        cwd=cwd,
    )
    input_sha256 = (
        sha256_file(protocol.request_path) if protocol.request_path.is_file() else None
    )
    log_sha256 = sha256_file(protocol.log_path) if protocol.log_path.is_file() else None
    if protocol.status == "unavailable":
        return apply_tolerance_contract(
            {
                "status": "unavailable",
                "reason": protocol.reason,
                "comparison_mode": request["mode"],
                "left_label": request["left_label"],
                "right_label": request["right_label"],
                "products": {},
                "input": str(protocol.request_path),
                "input_sha256": input_sha256,
                "output": str(protocol.output_path),
                "output_sha256": protocol.output_sha256,
                "log": str(protocol.log_path),
                "log_sha256": log_sha256,
            },
            request,
        )
    if protocol.status != "completed" or protocol.output is None:
        return apply_tolerance_contract(
            {
                "status": "failed_execution",
                "reason": protocol.reason,
                "return_code": protocol.return_code,
                "comparison_mode": request["mode"],
                "left_label": request["left_label"],
                "right_label": request["right_label"],
                "products": {},
                "input": str(protocol.request_path),
                "input_sha256": input_sha256,
                "output": str(protocol.output_path),
                "output_sha256": protocol.output_sha256,
                "log": str(protocol.log_path),
                "log_sha256": log_sha256,
            },
            request,
        )
    comparison = protocol.output
    try:
        validate_comparison_output(comparison, request)
    except ValueError as error:
        return apply_tolerance_contract(
            {
                "status": "failed_validation",
                "reason": str(error),
                "failure": {
                    "kind": "comparison_protocol_binding",
                    "reason": str(error),
                },
                "comparison_mode": request["mode"],
                "left_label": request["left_label"],
                "right_label": request["right_label"],
                "products": {},
                "input": str(protocol.request_path),
                "input_sha256": input_sha256,
                "output": str(protocol.output_path),
                "output_sha256": protocol.output_sha256,
                "log": str(protocol.log_path),
                "log_sha256": log_sha256,
            },
            request,
        )
    comparison["input"] = str(protocol.request_path)
    comparison["input_sha256"] = input_sha256
    comparison["output"] = str(protocol.output_path)
    comparison["output_sha256"] = protocol.output_sha256
    comparison["log"] = str(protocol.log_path)
    comparison["log_sha256"] = log_sha256
    comparison = apply_tolerance_contract(comparison, request)
    if request["mode"] == "full":
        rejected = _full_comparison_rejection(comparison)
        if rejected is not None:
            if comparison.get("status") == "completed":
                comparison["status"] = "structure_review_not_accepted"
                comparison["reason"] = rejected
                comparison["failure"] = {
                    "kind": "structured_difference_review",
                    "reason": rejected,
                }
            _record_structure_workspace_failure(
                request, comparison.get("reason") or rejected
            )
        else:
            try:
                _remove_accepted_structure_workspace(request)
            except ValueError as error:
                reason = f"accepted comparison workspace cleanup failed: {error}"
                comparison["status"] = "workspace_cleanup_failed"
                comparison["reason"] = reason
                comparison["failure"] = {
                    "kind": "structure_workspace_cleanup",
                    "reason": reason,
                }
                _record_structure_workspace_failure(request, reason)
    return comparison


def _full_comparison_rejection(comparison: dict[str, Any]) -> str | None:
    if comparison.get("status") != "completed":
        return comparison.get("reason") or "full comparison did not complete"
    products = comparison.get("products")
    if not isinstance(products, dict) or not products:
        return "full comparison produced no product reviews"
    accepted = {"good", "not_applicable_exact_zero"}
    rejected = {
        suffix: _structure_review_label(product)
        for suffix, product in products.items()
        if _structure_review_label(product) not in accepted
    }
    if rejected:
        details = ", ".join(
            f"{suffix}={label or 'missing'}"
            for suffix, label in sorted(rejected.items())
        )
        return "full comparison has non-accepted structured review(s): " + details
    return None


def _structure_review_label(product: Any) -> Any:
    if not isinstance(product, dict):
        return None
    structure = product.get("structured_difference")
    if not isinstance(structure, dict):
        return None
    review = structure.get("review")
    return review.get("label") if isinstance(review, dict) else None


def _structure_product_workspace(root: pathlib.Path, suffix: str) -> pathlib.Path:
    safe_suffix = suffix.strip(".").replace(".", "_") or "image"
    digest = hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:12]
    return root / f"{safe_suffix}-{digest}"


def _record_structure_workspace_failure(request: dict[str, Any], reason: str) -> None:
    root = pathlib.Path(request["structure_workspace_dir"])
    if not root.is_dir() or root.is_symlink():
        return
    marker = root / "failure.json"
    if marker.exists() or marker.is_symlink():
        return
    marker.write_text(
        json.dumps(
            {
                "status": "retained_exact_structure_workspace",
                "reason": str(reason),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _remove_accepted_structure_workspace(request: dict[str, Any]) -> None:
    root = pathlib.Path(request["structure_workspace_dir"])
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise ValueError("structure workspace is missing, non-directory, or a symlink")
    expected_directories = {
        _structure_product_workspace(root, suffix) for suffix in request["products"]
    }
    observed = set(root.iterdir())
    if observed != expected_directories:
        raise ValueError("structure workspace product inventory is not exact")
    expected_files = {"left.f64", "right.f64", "diff.f64", "coverage.u8"}
    for directory in sorted(expected_directories):
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError("structure product workspace is unavailable or unsafe")
        children = set(directory.iterdir())
        if {path.name for path in children} != expected_files:
            raise ValueError("structure product workspace file inventory is not exact")
        if any(not path.is_file() or path.is_symlink() for path in children):
            raise ValueError("structure product workspace contains an unsafe file")
    for directory in sorted(expected_directories):
        for name in sorted(expected_files):
            (directory / name).unlink()
        directory.rmdir()
    root.rmdir()


def apply_tolerance_contract(
    comparison: dict[str, Any], request: dict[str, Any]
) -> dict[str, Any]:
    """Persist and enforce the frozen numerical contract on every comparison."""

    contract = request.get("tolerances")
    if contract is None:
        return comparison
    if comparison.get("status") in {
        "unavailable",
        "failed_execution",
        "failed_validation",
    }:
        # No numerical evidence exists to evaluate. Preserve the operational
        # failure classification so the run-result layer can publish its
        # closed terminal summary instead of mislabeling it as a tolerance
        # result.
        return comparison
    comparison["tolerances"] = contract
    evaluation = evaluate_comparison_tolerances(comparison, contract)
    comparison["tolerance_evaluation"] = evaluation
    if evaluation["status"] != "passed":
        comparison["status"] = (
            "out_of_tolerance"
            if evaluation["status"] == "failed"
            else "comparison_incomplete"
        )
        names = evaluation["failed_checks"] or evaluation["incomplete_checks"]
        comparison["reason"] = (
            f"frozen tolerance evaluation {evaluation['status']}: " + ", ".join(names)
        )
    return comparison
