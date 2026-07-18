#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
import json
import math
import os
import sys

import numpy as np
from casatools import image

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception as error:
    plt = None
    MATPLOTLIB_ERROR = str(error)
else:
    MATPLOTLIB_ERROR = None


def main():
    with open(sys.argv[1], "r", encoding="utf-8") as handle:
        request = json.load(handle)
    products = {}
    os.makedirs(request["panel_dir"], exist_ok=True)
    max_elements = int(request["max_elements_per_product"])
    beam_info = estimate_beam_info(request["casa_prefix"] + ".psf", max_elements)
    panel_displays = product_panel_displays(request, max_elements)
    for suffix in request["products"]:
        rust_path = request["rust_prefix"] + suffix
        casa_path = request["casa_prefix"] + suffix
        products[suffix] = compare_one(
            rust_path,
            casa_path,
            max_elements,
            request["panel_dir"],
            suffix,
            beam_info,
            panel_displays.get(suffix),
        )
    output = {
        "status": "completed",
        "beam_info": beam_info,
        "products": products,
        "structured_difference_review": summarize_product_reviews(products),
    }
    with open(sys.argv[2], "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2, sort_keys=True)
        handle.write("\n")


def summarize_product_reviews(products):
    product_labels = {}
    product_summaries = {}
    check_labels = {}
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
        for check in review.get("checks", []):
            if not isinstance(check, dict):
                continue
            name = check.get("name")
            if not isinstance(name, str):
                continue
            check_labels.setdefault(name, {})[suffix] = check.get("label", "unknown")
    overall = worst_review_label(product_labels.values())
    return {
        "label": overall,
        "summary": structured_difference_rollup_summary(overall, product_labels),
        "products": product_labels,
        "product_summaries": product_summaries,
        "checks_by_product": check_labels,
        "thresholds": structured_difference_thresholds(),
        "legend": structured_difference_review_legend(),
    }


def structured_difference_rollup_summary(overall, product_labels):
    if not product_labels:
        return "No structured-difference product reviews were available."
    grouped = {
        label: [suffix for suffix, product_label in product_labels.items() if product_label == label]
        for label in ("bad", "investigate", "good", "unknown")
    }
    parts = []
    for label in ("bad", "investigate", "good", "unknown"):
        suffixes = grouped[label]
        if suffixes:
            parts.append(f"{label}: {', '.join(suffixes)}")
    return f"overall {overall}; " + "; ".join(parts)


def product_panel_displays(request, max_elements):
    displays = {}
    if ".model" not in request["products"]:
        return displays
    restored = restored_model_panel_display(
        rust_prefix=request["rust_prefix"],
        casa_prefix=request["casa_prefix"],
        max_elements=max_elements,
    )
    if restored is not None:
        displays[".model"] = restored
    return displays


def restored_model_panel_display(rust_prefix, casa_prefix, max_elements):
    required = {
        "rust_image": rust_prefix + ".image",
        "rust_residual": rust_prefix + ".residual",
        "casa_image": casa_prefix + ".image",
        "casa_residual": casa_prefix + ".residual",
    }
    missing = [path for path in required.values() if not os.path.isdir(path)]
    if missing:
        return {
            "status": "unavailable",
            "reason": "restored model visualization requires .image and .residual",
            "missing_paths": missing,
        }
    try:
        rust_image = load_image_display_plane(required["rust_image"], max_elements)
        rust_residual = load_image_display_plane(required["rust_residual"], max_elements)
        casa_image = load_image_display_plane(required["casa_image"], max_elements)
        casa_residual = load_image_display_plane(required["casa_residual"], max_elements)
    except Exception as error:
        return {
            "status": "unavailable",
            "reason": f"failed to load restored model visualization inputs: {error}",
        }
    inputs = [rust_image, rust_residual, casa_image, casa_residual]
    shapes = [item["shape"] for item in inputs]
    strides = [item["sample_stride"] for item in inputs]
    if any(shape != shapes[0] for shape in shapes):
        return {
            "status": "unavailable",
            "reason": "restored model visualization inputs have mismatched shapes",
            "shapes": shapes,
        }
    if any(stride != strides[0] for stride in strides):
        return {
            "status": "unavailable",
            "reason": "restored model visualization inputs have mismatched sampling strides",
            "sample_strides": strides,
        }
    rust_display = rust_image["data"] - rust_residual["data"]
    casa_display = casa_image["data"] - casa_residual["data"]
    return {
        "status": "available",
        "rust_data": rust_display,
        "casa_data": casa_display,
        "diff_data": rust_display - casa_display,
        "transform": "restored_model_from_image_minus_residual",
        "description": ".model visualized as restoring-beam-convolved model via .image - .residual",
        "product_label": ".model restored",
        "value_label": "Jy/beam",
        "shape": shapes[0],
        "sample_stride": strides[0],
    }


def compare_one(rust_path, casa_path, max_elements, panel_dir, suffix, beam_info, panel_display=None):
    if not os.path.isdir(rust_path) or not os.path.isdir(casa_path):
        return {
            "status": "missing",
            "rust_path": rust_path,
            "casa_path": casa_path,
            "rust_exists": os.path.isdir(rust_path),
            "casa_exists": os.path.isdir(casa_path),
        }
    rust = load_image(rust_path, max_elements)
    casa = load_image(casa_path, max_elements)
    if rust["shape"] != casa["shape"]:
        return {
            "status": "shape_mismatch",
            "rust_path": rust_path,
            "casa_path": casa_path,
            "rust_shape": rust["shape"],
            "casa_shape": casa["shape"],
        }
    rust_data = rust["data"]
    casa_data = casa["data"]
    mask = np.isfinite(rust_data) & np.isfinite(casa_data)
    valid_count = int(np.count_nonzero(mask))
    if valid_count == 0:
        return {
            "status": "no_finite_overlap",
            "rust_path": rust_path,
            "casa_path": casa_path,
            "shape": rust["shape"],
            "sample_stride": rust["sample_stride"],
            "sampled_elements": int(rust_data.size),
        }
    rust_valid = rust_data[mask]
    casa_valid = casa_data[mask]
    diff = rust_valid - casa_valid
    casa_peak = max(abs(float(np.nanmin(casa_valid))), abs(float(np.nanmax(casa_valid))))
    casa_rms = rms(casa_valid)
    diff_rms = rms(diff)
    diff_abs_max = float(np.nanmax(np.abs(diff)))
    correlation = correlation_value(rust_valid, casa_valid)
    rust_peak = peak_summary(rust_data)
    casa_peak_summary = peak_summary(casa_data)
    diff_full = rust_data - casa_data
    diff_peak = peak_summary(diff_full)
    structure = structured_difference_metrics(
        suffix=suffix,
        rust_data=rust_data,
        casa_data=casa_data,
        diff_data=diff_full,
        beam_info=beam_info,
    )
    panel_display_data = panel_display
    if panel_display_data is None:
        rust_display = load_image_display_plane(rust_path, max_elements)
        casa_display = load_image_display_plane(casa_path, max_elements)
        panel_display_data = {
            "status": "available",
            "rust_data": rust_display["data"],
            "casa_data": casa_display["data"],
            "diff_data": rust_display["data"] - casa_display["data"],
            "transform": "center_plane_full_spatial_display",
            "description": (
                "center display plane loaded with spatial-only stride; "
                "non-spatial axes fixed at their center"
            ),
            "shape": rust_display["shape"],
            "display_bounds": rust_display["display_bounds"],
            "sample_stride": rust_display["sample_stride"],
        }
    panel = write_review_panel(
        panel_dir=panel_dir,
        suffix=suffix,
        rust_data=rust_data,
        casa_data=casa_data,
        diff_data=diff_full,
        review=structure.get("review") if isinstance(structure, dict) else None,
        display=panel_display_data,
    )
    return {
        "status": "compared",
        "rust_path": rust_path,
        "casa_path": casa_path,
        "shape": rust["shape"],
        "sample_stride": rust["sample_stride"],
        "sampled_elements": int(rust_data.size),
        "finite_overlap": valid_count,
        "rust_min": finite_float(np.nanmin(rust_valid)),
        "rust_max": finite_float(np.nanmax(rust_valid)),
        "rust_rms": finite_float(rms(rust_valid)),
        "casa_min": finite_float(np.nanmin(casa_valid)),
        "casa_max": finite_float(np.nanmax(casa_valid)),
        "casa_rms": finite_float(casa_rms),
        "diff_abs_max": finite_float(diff_abs_max),
        "diff_rms": finite_float(diff_rms),
        "diff_rms_over_casa_rms": finite_float(diff_rms / abs(casa_rms)) if casa_rms else None,
        "diff_abs_max_over_casa_peak": finite_float(diff_abs_max / casa_peak) if casa_peak else None,
        "correlation": finite_float(correlation) if correlation is not None else None,
        "rust_peak_abs": rust_peak,
        "casa_peak_abs": casa_peak_summary,
        "diff_peak_abs": diff_peak,
        "structured_difference": structure,
        "review_panel": panel,
    }


def estimate_beam_info(psf_path, max_elements):
    if not os.path.isdir(psf_path):
        return {"status": "missing_psf", "psf_path": psf_path}
    try:
        psf = load_image(psf_path, max_elements)
    except Exception as error:
        return {"status": "failed", "psf_path": psf_path, "reason": str(error)}
    plane = display_plane(psf["data"])
    finite = np.isfinite(plane)
    if not np.any(finite):
        return {"status": "no_finite_psf", "psf_path": psf_path}
    peak_index = np.unravel_index(int(np.nanargmax(np.abs(plane))), plane.shape)
    peak_abs = float(abs(plane[peak_index]))
    if not np.isfinite(peak_abs) or peak_abs <= 0.0:
        return {"status": "zero_psf_peak", "psf_path": psf_path}
    half = 0.5 * peak_abs
    x_width = contiguous_threshold_width(np.abs(plane[:, peak_index[1]]), peak_index[0], half)
    y_width = contiguous_threshold_width(np.abs(plane[peak_index[0], :]), peak_index[1], half)
    beam_area = math.pi * x_width * y_width / (4.0 * math.log(2.0))
    block_side = max(1, int(round(math.sqrt(max(1.0, beam_area)))))
    return {
        "status": "estimated_from_psf",
        "psf_path": psf_path,
        "sample_stride": psf["sample_stride"],
        "peak_location": [int(value) for value in peak_index],
        "peak_abs": finite_float(peak_abs),
        "fwhm_pixels": [int(x_width), int(y_width)],
        "beam_area_pixels": finite_float(beam_area),
        "beam_block_side_pixels": int(block_side),
    }


def contiguous_threshold_width(values, center, threshold):
    center = int(center)
    lower = center
    upper = center
    while lower > 0 and values[lower - 1] >= threshold:
        lower -= 1
    while upper + 1 < values.size and values[upper + 1] >= threshold:
        upper += 1
    return int(upper - lower + 1)


def structured_difference_metrics(suffix, rust_data, casa_data, diff_data, beam_info):
    rust_plane = display_plane(rust_data)
    casa_plane = display_plane(casa_data)
    diff_plane = display_plane(diff_data)
    base_mask = np.isfinite(rust_plane) & np.isfinite(casa_plane) & np.isfinite(diff_plane)
    mask, mask_description = structured_difference_mask(suffix, rust_plane, casa_plane, base_mask)
    finite_count = int(np.count_nonzero(mask))
    if finite_count == 0:
        return {
            "status": "no_masked_pixels",
            "mask": mask_description,
            "finite_overlap": int(np.count_nonzero(base_mask)),
            "beam_info_status": beam_info.get("status") if isinstance(beam_info, dict) else None,
        }

    beam_side = 1
    if isinstance(beam_info, dict) and beam_info.get("status") == "estimated_from_psf":
        beam_side = max(1, int(beam_info.get("beam_block_side_pixels") or 1))
    analysis_mask = erode_mask_for_product(mask, suffix, beam_side)
    if not np.any(analysis_mask):
        analysis_mask = mask

    diff_values = diff_plane[analysis_mask]
    casa_values = casa_plane[analysis_mask]
    flux_norm = robust_product_scale(casa_values)
    diff_rms = rms(diff_values)
    normalized_diff_rms = diff_rms / flux_norm if flux_norm else None
    if non_spatial_product(suffix):
        classification = non_spatial_difference_classification(normalized_diff_rms)
        review = structured_difference_review(
            suffix=suffix,
            classification=classification,
            normalized_diff_rms=normalized_diff_rms,
            low_order_r2=None,
            large_scale_power=None,
            block_decay_slope=None,
        )
        return {
            "status": "computed",
            "mask": mask_description,
            "masked_pixels": finite_count,
            "analysis_pixels": int(np.count_nonzero(analysis_mask)),
            "beam_block_side_pixels": int(beam_side),
            "normalization": {
                "type": "casa_support_rms_or_peak",
                "value": finite_float(flux_norm),
            },
            "diff_rms": finite_float(diff_rms),
            "normalized_diff_rms": finite_float(normalized_diff_rms),
            "low_order_r2_quadratic": None,
            "large_scale_power_fraction": None,
            "scale_offset_gradient_fit": {
                "status": "not_applicable",
                "reason": "non_spatial_product",
            },
            "beam_block_rms_by_scale": [],
            "block_rms_decay_slope_vs_independent_beams": None,
            "classification": classification,
            "review": review,
        }
    low_order_r2 = low_order_r2_score(diff_plane, analysis_mask)
    large_scale_power = large_scale_power_fraction(
        diff_plane,
        analysis_mask,
        beam_side,
        min_wavelength_beams=8.0,
    )
    basis_fit = difference_basis_fit(casa_plane, diff_plane, analysis_mask)
    block_metrics = beam_block_metrics(diff_plane, analysis_mask, beam_side, flux_norm)
    classification = structured_difference_classification(
        normalized_diff_rms=normalized_diff_rms,
        low_order_r2=low_order_r2,
        large_scale_power=large_scale_power,
        block_decay_slope=block_metrics["decay_slope"],
    )
    review = structured_difference_review(
        suffix=suffix,
        classification=classification,
        normalized_diff_rms=normalized_diff_rms,
        low_order_r2=low_order_r2,
        large_scale_power=large_scale_power,
        block_decay_slope=block_metrics["decay_slope"],
    )
    return {
        "status": "computed",
        "mask": mask_description,
        "masked_pixels": finite_count,
        "analysis_pixels": int(np.count_nonzero(analysis_mask)),
        "beam_block_side_pixels": int(beam_side),
        "normalization": {
            "type": "casa_support_rms_or_peak",
            "value": finite_float(flux_norm),
        },
        "diff_rms": finite_float(diff_rms),
        "normalized_diff_rms": finite_float(normalized_diff_rms),
        "low_order_r2_quadratic": finite_float(low_order_r2),
        "large_scale_power_fraction": large_scale_power,
        "scale_offset_gradient_fit": basis_fit,
        "beam_block_rms_by_scale": block_metrics["scales"],
        "block_rms_decay_slope_vs_independent_beams": block_metrics["decay_slope"],
        "classification": classification,
        "review": review,
    }


def structured_difference_mask(suffix, rust_plane, casa_plane, base_mask):
    if suffix == ".weight":
        scale = max(
            finite_absmax(rust_plane[base_mask]),
            finite_absmax(casa_plane[base_mask]),
        )
        if scale > 0.0:
            threshold = 1.0e-3 * scale
            return (
                base_mask & ((np.abs(rust_plane) > threshold) | (np.abs(casa_plane) > threshold)),
                {
                    "type": "weight_union_support",
                    "threshold_fraction_of_peak": 1.0e-3,
                    "threshold": finite_float(threshold),
                },
            )
    if suffix == ".pb":
        return (
            base_mask & (casa_plane > 0.01),
            {
                "type": "casa_pb_support",
                "threshold": 0.01,
            },
        )
    return base_mask, {"type": "finite_overlap"}


def non_spatial_product(suffix):
    return suffix in {".sumwt"}


def erode_mask_for_product(mask, suffix, beam_side):
    if suffix not in {".pb", ".weight"} or beam_side <= 1:
        return mask
    radius = int(max(1, round(beam_side)))
    if mask.shape[0] <= 2 * radius or mask.shape[1] <= 2 * radius:
        return mask
    eroded = np.zeros_like(mask, dtype=bool)
    eroded[radius:-radius, radius:-radius] = mask[radius:-radius, radius:-radius]
    return eroded


def robust_product_scale(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0
    scale = rms(finite)
    if scale > 0.0 and np.isfinite(scale):
        return float(abs(scale))
    return finite_absmax(finite)


def finite_absmax(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0
    return float(np.nanmax(np.abs(finite)))


def low_order_r2_score(data, mask):
    if int(np.count_nonzero(mask)) < 8:
        return None
    y_index, x_index = np.indices(data.shape)
    x_values = x_index[mask].astype(np.float64)
    y_values = y_index[mask].astype(np.float64)
    x_span = float(np.ptp(x_values))
    y_span = float(np.ptp(y_values))
    x = (x_values - float(np.mean(x_values))) / (0.5 * x_span if x_span else 1.0)
    y = (y_values - float(np.mean(y_values))) / (0.5 * y_span if y_span else 1.0)
    z = data[mask].astype(np.float64)
    z = z - float(np.mean(z))
    total = float(np.sum(z * z))
    if total <= 0.0 or not np.isfinite(total):
        return None
    basis = np.vstack([np.ones_like(x), x, y, x * x, x * y, y * y, x * x + y * y]).T
    coefficients, *_ = np.linalg.lstsq(basis, z, rcond=None)
    fitted = basis @ coefficients
    residual = z - fitted
    return 1.0 - float(np.sum(residual * residual)) / total


def difference_basis_fit(reference, diff, mask):
    if int(np.count_nonzero(mask)) < 8:
        return {"status": "insufficient_pixels"}
    if reference.ndim != 2 or min(reference.shape) < 2:
        return {
            "status": "insufficient_dimensions",
            "shape": [int(v) for v in reference.shape],
        }
    y_gradient, x_gradient = np.gradient(reference.astype(np.float64))
    diff_values = diff[mask].astype(np.float64)
    reference_values = reference[mask].astype(np.float64)
    basis = np.vstack(
        [
            reference_values,
            np.ones_like(reference_values),
            x_gradient[mask].astype(np.float64),
            y_gradient[mask].astype(np.float64),
        ]
    ).T
    coefficients, *_ = np.linalg.lstsq(basis, diff_values, rcond=None)
    fitted = basis @ coefficients
    residual = diff_values - fitted
    total = float(np.sum((diff_values - float(np.mean(diff_values))) ** 2))
    residual_sum = float(np.sum(residual * residual))
    r2 = 1.0 - residual_sum / total if total > 0.0 and np.isfinite(total) else None
    return {
        "status": "computed",
        "model": "diff ~= scale*reference + offset + dx*d_reference_dx + dy*d_reference_dy",
        "r2": finite_float(r2),
        "diff_rms": finite_float(rms(diff_values)),
        "residual_rms": finite_float(rms(residual)),
        "coefficients": {
            "scale": finite_float(coefficients[0]),
            "offset": finite_float(coefficients[1]),
            "dx_pixels": finite_float(coefficients[2]),
            "dy_pixels": finite_float(coefficients[3]),
        },
    }


def structured_difference_classification(
    normalized_diff_rms,
    low_order_r2,
    large_scale_power,
    block_decay_slope,
):
    amplitude = classify_amplitude(normalized_diff_rms)
    structure_components = {
        "block_rms_decay_slope_vs_independent_beams": classify_block_decay(block_decay_slope),
        "large_scale_power_fraction": classify_large_scale_power(
            large_scale_power.get("fraction") if isinstance(large_scale_power, dict) else None
        ),
        "low_order_r2_quadratic": classify_low_order_r2(low_order_r2),
    }
    numerical_floor_override = (
        normalized_diff_rms is not None and normalized_diff_rms < 1.0e-6
    )
    if numerical_floor_override and amplitude == "good":
        structure = "good"
        overall = "good"
    else:
        structure = worst_classification(structure_components.values())
        overall = overall_structured_difference_label(amplitude, structure)
    return {
        "overall": overall,
        "amplitude": amplitude,
        "structure": structure,
        "structure_components": structure_components,
        "structure_suppressed_by_numerical_floor": numerical_floor_override,
        "thresholds": structured_difference_thresholds(),
    }


def non_spatial_difference_classification(normalized_diff_rms):
    amplitude = classify_amplitude(normalized_diff_rms)
    return {
        "overall": amplitude,
        "amplitude": amplitude,
        "structure": "not_applicable",
        "structure_components": {},
        "thresholds": structured_difference_thresholds(),
    }


def structured_difference_review(
    suffix,
    classification,
    normalized_diff_rms,
    low_order_r2,
    large_scale_power,
    block_decay_slope,
):
    large_scale_fraction = (
        large_scale_power.get("fraction") if isinstance(large_scale_power, dict) else None
    )
    components = classification.get("structure_components", {})
    checks = [
        {
            "name": "normalized_diff_rms",
            "label": classification.get("amplitude", "unknown"),
            "value": finite_float(normalized_diff_rms),
            "meaning": "beam/product-scale RMS amplitude difference",
        },
        {
            "name": "block_rms_decay_slope_vs_independent_beams",
            "label": components.get(
                "block_rms_decay_slope_vs_independent_beams", "unknown"
            ),
            "value": finite_float(block_decay_slope),
            "meaning": "whether averaging over independent beams suppresses the difference",
        },
        {
            "name": "large_scale_power_fraction",
            "label": components.get("large_scale_power_fraction", "unknown"),
            "value": finite_float(large_scale_fraction),
            "meaning": "fraction of difference power on scales much larger than the beam",
        },
        {
            "name": "low_order_r2_quadratic",
            "label": components.get("low_order_r2_quadratic", "unknown"),
            "value": finite_float(low_order_r2),
            "meaning": "fraction of difference variance explained by a smooth quadratic surface",
        },
    ]
    return {
        "label": classification.get("overall", "unknown"),
        "summary": structured_difference_review_summary(suffix, classification),
        "checks": checks,
        "legend": structured_difference_review_legend(),
    }


def structured_difference_thresholds():
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


def structured_difference_review_legend():
    return {
        "good": "No review action expected from this check.",
        "investigate": "Plausible but needs review in context.",
        "bad": "Structured or large enough difference; do not close without explanation.",
        "unknown": "Check could not be evaluated for this product.",
    }


def structured_difference_review_summary(suffix, classification):
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
            return f"{suffix}: investigate; non-spatial product amplitude is {amplitude}."
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


def classify_amplitude(value):
    if value is None:
        return "unknown"
    if value < 1.0e-4:
        return "good"
    if value <= 1.0e-3:
        return "investigate"
    return "bad"


def classify_block_decay(value):
    if value is None:
        return "unknown"
    if value <= -0.35:
        return "good"
    if value <= -0.15:
        return "investigate"
    return "bad"


def classify_large_scale_power(value):
    if value is None:
        return "unknown"
    if value < 0.25:
        return "good"
    if value <= 0.5:
        return "investigate"
    return "bad"


def classify_low_order_r2(value):
    if value is None:
        return "unknown"
    if value < 0.05:
        return "good"
    if value <= 0.2:
        return "investigate"
    return "bad"


def worst_classification(labels):
    rank = {"unknown": 0, "good": 1, "investigate": 2, "bad": 3}
    labels = list(labels)
    if not labels:
        return "unknown"
    return max(labels, key=lambda label: rank.get(label, 0))


def worst_review_label(labels):
    rank = {"unknown": 0, "good": 1, "investigate": 2, "bad": 3}
    labels = list(labels)
    if not labels:
        return "unknown"
    return max(labels, key=lambda label: rank.get(label, 0))


def overall_structured_difference_label(amplitude, structure):
    if amplitude == "bad":
        return "bad"
    if amplitude == "investigate" and structure == "bad":
        return "bad"
    if amplitude == "good" and structure == "good":
        return "good"
    if amplitude == "unknown" and structure == "unknown":
        return "unknown"
    return "investigate"


def large_scale_power_fraction(data, mask, beam_side, min_wavelength_beams):
    if int(np.count_nonzero(mask)) < 4:
        return None
    centered = np.asarray(data, dtype=np.float64).copy()
    centered[~mask] = np.nan
    mean = float(np.nanmean(centered))
    centered = np.where(np.isfinite(centered), centered - mean, 0.0)
    spectrum = np.fft.rfft2(centered)
    power = np.abs(spectrum) ** 2
    y_freq = np.fft.fftfreq(centered.shape[0])[:, np.newaxis]
    x_freq = np.fft.rfftfreq(centered.shape[1])[np.newaxis, :]
    radius = np.sqrt(x_freq * x_freq + y_freq * y_freq)
    dc = radius == 0.0
    total = float(np.sum(power[~dc]))
    if total <= 0.0 or not np.isfinite(total):
        return None
    cutoff = 1.0 / max(1.0, float(beam_side) * float(min_wavelength_beams))
    selected = (radius <= cutoff) & (~dc)
    return {
        "min_wavelength_beams": finite_float(min_wavelength_beams),
        "frequency_cutoff_cycles_per_pixel": finite_float(cutoff),
        "fraction": finite_float(float(np.sum(power[selected])) / total),
    }


def beam_block_metrics(data, mask, beam_side, flux_norm):
    scales = []
    block_sides = []
    normalized_rms_values = []
    independent_beam_counts = []
    for multiplier in [1, 2, 4, 8, 16, 32]:
        side = max(1, int(round(float(beam_side) * multiplier)))
        metric = block_metric_for_side(data, mask, side, beam_side, flux_norm, multiplier)
        if metric is None:
            continue
        scales.append(metric)
        normalized = metric.get("normalized_block_mean_rms")
        independent_beams = metric.get("approx_independent_beams_per_block")
        if normalized is not None and normalized > 0.0 and independent_beams and independent_beams > 0.0:
            block_sides.append(side)
            normalized_rms_values.append(float(normalized))
            independent_beam_counts.append(float(independent_beams))
    slope = None
    if len(normalized_rms_values) >= 2:
        x = np.log(np.asarray(independent_beam_counts, dtype=np.float64))
        y = np.log(np.asarray(normalized_rms_values, dtype=np.float64))
        if np.all(np.isfinite(x)) and np.all(np.isfinite(y)) and np.ptp(x) > 0.0:
            slope = float(np.polyfit(x, y, 1)[0])
    return {"scales": scales, "decay_slope": finite_float(slope) if slope is not None else None}


def block_metric_for_side(data, mask, side, beam_side, flux_norm, multiplier):
    height, width = data.shape
    means = []
    pixel_rms_values = []
    min_pixels = max(4, int(math.ceil(0.35 * side * side)))
    for y_start in range(0, height, side):
        for x_start in range(0, width, side):
            block_mask = mask[y_start : y_start + side, x_start : x_start + side]
            if int(np.count_nonzero(block_mask)) < min_pixels:
                continue
            block = data[y_start : y_start + side, x_start : x_start + side][block_mask]
            means.append(float(np.mean(block)))
            pixel_rms_values.append(rms(block))
    if len(means) < 3:
        return None
    mean_values = np.asarray(means, dtype=np.float64)
    pixel_rms_mean = float(np.mean(pixel_rms_values)) if pixel_rms_values else None
    block_mean_rms = rms(mean_values)
    robust_center = float(np.median(mean_values))
    robust_sigma = 1.4826 * float(np.median(np.abs(mean_values - robust_center)))
    max_robust_z = None
    if robust_sigma > 0.0 and np.isfinite(robust_sigma):
        max_robust_z = float(np.nanmax(np.abs(mean_values - robust_center)) / robust_sigma)
    independent_beams = (float(side) / max(1.0, float(beam_side))) ** 2
    return {
        "beam_width_multiplier": finite_float(multiplier),
        "block_side_pixels": int(side),
        "approx_independent_beams_per_block": finite_float(independent_beams),
        "n_blocks": int(len(means)),
        "block_mean_rms": finite_float(block_mean_rms),
        "normalized_block_mean_rms": finite_float(block_mean_rms / flux_norm) if flux_norm else None,
        "median_abs_block_mean": finite_float(float(np.median(np.abs(mean_values)))),
        "mean_pixel_rms_in_blocks": finite_float(pixel_rms_mean),
        "block_mean_rms_over_mean_pixel_rms": finite_float(block_mean_rms / pixel_rms_mean)
        if pixel_rms_mean
        else None,
        "max_block_robust_z": finite_float(max_robust_z) if max_robust_z is not None else None,
    }


def correlation_value(left, right):
    if left.size < 2 or right.size < 2:
        return None
    left_std = float(np.nanstd(left))
    right_std = float(np.nanstd(right))
    if left_std == 0.0 or right_std == 0.0:
        return None
    return float(np.corrcoef(left.ravel(), right.ravel())[0, 1])


def peak_summary(data):
    finite = np.isfinite(data)
    if not np.any(finite):
        return None
    filled = np.where(finite, np.abs(data), -np.inf)
    index = np.unravel_index(int(np.nanargmax(filled)), data.shape)
    return {
        "location": [int(value) for value in index],
        "value": finite_float(data[index]),
        "abs_value": finite_float(abs(data[index])),
    }


def write_review_panel(panel_dir, suffix, rust_data, casa_data, diff_data, review=None, display=None):
    if plt is None:
        return {
            "status": "skipped",
            "reason": f"matplotlib unavailable: {MATPLOTLIB_ERROR}",
        }
    display_status = "raw_product"
    display_transform = None
    display_description = None
    display_reason = None
    display_shape = None
    display_bounds = None
    display_sample_stride = None
    product_label = suffix if suffix else ".image"
    value_label = product_value_label(suffix)
    panel_rust_data = rust_data
    panel_casa_data = casa_data
    panel_diff_data = diff_data
    if isinstance(display, dict):
        if display.get("status") == "available":
            panel_rust_data = display["rust_data"]
            panel_casa_data = display["casa_data"]
            panel_diff_data = display["diff_data"]
            product_label = display.get("product_label") or product_label
            value_label = display.get("value_label") or value_label
            display_status = "derived"
            display_transform = display.get("transform")
            display_description = display.get("description")
            display_shape = display.get("shape")
            display_bounds = display.get("display_bounds")
            display_sample_stride = display.get("sample_stride")
        else:
            display_status = display.get("status", "unavailable")
            display_reason = display.get("reason")
    rust_plane = display_plane(panel_rust_data)
    casa_plane = display_plane(panel_casa_data)
    diff_plane = display_plane(panel_diff_data)
    shared = np.concatenate(
        [
            rust_plane[np.isfinite(rust_plane)].ravel(),
            casa_plane[np.isfinite(casa_plane)].ravel(),
        ]
    )
    if shared.size == 0:
        return {"status": "skipped", "reason": "no finite pixels for panel scaling"}
    image_vmin, image_vmax = panel_color_limits(shared)
    finite_diff = diff_plane[np.isfinite(diff_plane)]
    diff_abs = panel_symmetric_abs_limit(finite_diff)
    safe_name = suffix.strip(".").replace(".", "_") or "image"
    review_label = None
    review_summary = None
    if isinstance(review, dict):
        review_label = review.get("label")
        review_summary = review.get("summary")
    panel_path = os.path.join(panel_dir, f"{safe_name}.review.png")
    render_review_panel_figure(
        panel_path=panel_path,
        rust_plane=rust_plane,
        casa_plane=casa_plane,
        diff_plane=diff_plane,
        product_label=product_label,
        value_label=value_label,
        image_vmin=image_vmin,
        image_vmax=image_vmax,
        diff_abs=diff_abs,
        review_label=review_label,
    )
    zoom_panel = write_zoom_review_panel(
        panel_dir=panel_dir,
        safe_name=safe_name,
        rust_plane=rust_plane,
        casa_plane=casa_plane,
        diff_plane=diff_plane,
        product_label=product_label,
        value_label=value_label,
        review_label=review_label,
    )
    return {
        "status": "written",
        "path": panel_path,
        "casa_rs_and_casa_color_limits": [image_vmin, image_vmax],
        "difference_color_limits": [-diff_abs, diff_abs],
        "structured_difference_label": review_label,
        "structured_difference_summary": review_summary,
        "display_status": display_status,
        "display_transform": display_transform,
        "display_description": display_description,
        "display_reason": display_reason,
        "display_shape": display_shape,
        "display_bounds": display_bounds,
        "display_sample_stride": display_sample_stride,
        "zoom_panel": zoom_panel,
    }


def write_zoom_review_panel(
    panel_dir,
    safe_name,
    rust_plane,
    casa_plane,
    diff_plane,
    product_label,
    value_label,
    review_label,
):
    bounds = zoom_bounds_for_planes(rust_plane, casa_plane)
    if bounds is None:
        return {"status": "skipped", "reason": "no finite nonzero support for zoom panel"}
    x0, x1, y0, y1 = bounds
    if x0 == 0 and y0 == 0 and x1 == rust_plane.shape[0] and y1 == rust_plane.shape[1]:
        return {
            "status": "skipped",
            "reason": "zoom bounds cover the full review plane",
            "bounds": {"x_start": x0, "x_end": x1, "y_start": y0, "y_end": y1},
        }
    rust_zoom = rust_plane[x0:x1, y0:y1]
    casa_zoom = casa_plane[x0:x1, y0:y1]
    diff_zoom = diff_plane[x0:x1, y0:y1]
    shared = np.concatenate(
        [
            rust_zoom[np.isfinite(rust_zoom)].ravel(),
            casa_zoom[np.isfinite(casa_zoom)].ravel(),
        ]
    )
    if shared.size == 0:
        return {"status": "skipped", "reason": "no finite pixels for zoom panel scaling"}
    image_vmin, image_vmax = panel_color_limits(shared)
    finite_diff = diff_zoom[np.isfinite(diff_zoom)]
    diff_abs = panel_symmetric_abs_limit(finite_diff)
    zoom_path = os.path.join(panel_dir, f"{safe_name}.zoom.review.png")
    zoom_label = f"{product_label} zoom"
    render_review_panel_figure(
        panel_path=zoom_path,
        rust_plane=rust_zoom,
        casa_plane=casa_zoom,
        diff_plane=diff_zoom,
        product_label=zoom_label,
        value_label=value_label,
        image_vmin=image_vmin,
        image_vmax=image_vmax,
        diff_abs=diff_abs,
        review_label=review_label,
    )
    return {
        "status": "written",
        "path": zoom_path,
        "bounds": {"x_start": x0, "x_end": x1, "y_start": y0, "y_end": y1},
        "casa_rs_and_casa_color_limits": [image_vmin, image_vmax],
        "difference_color_limits": [-diff_abs, diff_abs],
    }


def render_review_panel_figure(
    panel_path,
    rust_plane,
    casa_plane,
    diff_plane,
    product_label,
    value_label,
    image_vmin,
    image_vmax,
    diff_abs,
    review_label,
):
    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.8), constrained_layout=True)
    if review_label:
        fig.suptitle(
            f"{product_label} structured difference: {review_label}",
            fontsize=11,
        )
    rust_artist = axes[0].imshow(
        rust_plane.T,
        origin="lower",
        vmin=image_vmin,
        vmax=image_vmax,
        aspect="equal",
    )
    axes[0].set_title(f"casa-rs {product_label}")
    casa_artist = axes[1].imshow(
        casa_plane.T,
        origin="lower",
        vmin=image_vmin,
        vmax=image_vmax,
        aspect="equal",
    )
    axes[1].set_title(f"CASA {product_label}")
    diff_artist = axes[2].imshow(
        diff_plane.T,
        origin="lower",
        vmin=-diff_abs,
        vmax=diff_abs,
        cmap="coolwarm",
        aspect="equal",
    )
    axes[2].set_title(f"difference {product_label}\n(casa-rs - CASA)")
    for axis in axes:
        axis.set_aspect("equal", adjustable="box")
        axis.set_box_aspect(1)
    fig.colorbar(rust_artist, ax=axes[0], fraction=0.046, pad=0.04, label=value_label)
    fig.colorbar(casa_artist, ax=axes[1], fraction=0.046, pad=0.04, label=value_label)
    fig.colorbar(
        diff_artist,
        ax=axes[2],
        fraction=0.046,
        pad=0.04,
        label=f"casa-rs - CASA ({value_label})",
    )
    for axis in axes:
        axis.set_xticks([])
        axis.set_yticks([])
    fig.savefig(panel_path, dpi=160)
    plt.close(fig)


def product_value_label(suffix):
    if suffix in {".image", ".residual", ".image.pbcor"}:
        return "Jy/beam"
    if suffix == ".model":
        return "Jy/pixel"
    if suffix == ".psf":
        return "PSF response"
    if suffix in {".pb", ".weight"}:
        return "relative weight"
    return "value"


def zoom_bounds_for_planes(rust_plane, casa_plane):
    if rust_plane.ndim != 2 or casa_plane.ndim != 2 or rust_plane.shape != casa_plane.shape:
        return None
    finite = np.isfinite(rust_plane) & np.isfinite(casa_plane)
    if not np.any(finite):
        return None
    amplitude = np.maximum(np.abs(rust_plane), np.abs(casa_plane))
    amplitude = np.where(finite, amplitude, 0.0)
    peak = finite_absmax(amplitude)
    if peak <= 0.0:
        return None
    support = amplitude >= peak * 1.0e-3
    if not np.any(support):
        peak_index = np.unravel_index(int(np.nanargmax(amplitude)), amplitude.shape)
        xs = np.asarray([peak_index[0]])
        ys = np.asarray([peak_index[1]])
    else:
        xs, ys = np.nonzero(support)
    height, width = rust_plane.shape
    x_min = int(np.min(xs))
    x_max = int(np.max(xs)) + 1
    y_min = int(np.min(ys))
    y_max = int(np.max(ys)) + 1
    support_side = max(x_max - x_min, y_max - y_min)
    min_side = min(min(height, width), max(32, min(height, width) // 16))
    side = min(min(height, width), max(min_side, support_side * 4))
    x_center = (x_min + x_max) // 2
    y_center = (y_min + y_max) // 2
    x0 = max(0, min(height - side, x_center - side // 2))
    y0 = max(0, min(width - side, y_center - side // 2))
    return int(x0), int(x0 + side), int(y0), int(y0 + side)


def panel_color_limits(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0, 0.0
    vmin = finite_float(np.nanmin(finite))
    vmax = finite_float(np.nanmax(finite))
    if vmin is None or vmax is None:
        return 0.0, 0.0
    if vmax > vmin:
        return vmin, vmax
    abs_peak = finite_absmax(finite)
    delta = abs_peak * 1.0e-6 if abs_peak > 0.0 else 1.0
    return vmin - delta, vmax + delta


def panel_symmetric_abs_limit(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 1.0
    abs_peak = finite_float(np.nanmax(np.abs(finite)))
    if abs_peak is None or abs_peak <= 0.0:
        return 1.0
    return abs_peak


def display_plane(data):
    plane = np.squeeze(data)
    while plane.ndim > 2:
        plane = plane[..., plane.shape[-1] // 2]
    if plane.ndim == 0:
        plane = np.asarray([[float(plane)]])
    elif plane.ndim == 1:
        plane = plane[:, np.newaxis]
    return np.asarray(plane, dtype=np.float64)


def load_image(path, max_elements):
    tool = image()
    try:
        tool.open(path)
        shape = [int(v) for v in tool.shape()]
        stride = stride_for(shape, max_elements)
        trc = [max(0, v - 1) for v in shape]
        data = tool.getchunk(
            blc=[0] * len(shape),
            trc=trc,
            inc=stride,
            dropdeg=False,
            getmask=False,
        )
    finally:
        tool.close()
    return {
        "shape": shape,
        "sample_stride": stride,
        "data": np.asarray(data, dtype=np.float64),
    }


def load_image_display_plane(path, max_elements):
    tool = image()
    try:
        tool.open(path)
        shape = [int(v) for v in tool.shape()]
        blc, trc = display_plane_bounds(shape)
        plane_shape = [shape[0] if shape else 1, shape[1] if len(shape) > 1 else 1]
        spatial_stride = stride_for(plane_shape, max_elements)
        inc = [1] * len(shape)
        if len(inc) >= 1:
            inc[0] = spatial_stride[0]
        if len(inc) >= 2:
            inc[1] = spatial_stride[1]
        data = tool.getchunk(
            blc=blc,
            trc=trc,
            inc=inc,
            dropdeg=False,
            getmask=False,
        )
    finally:
        tool.close()
    return {
        "shape": shape,
        "display_bounds": {
            "blc": blc,
            "trc": trc,
            "inc": inc,
        },
        "sample_stride": inc,
        "data": np.asarray(data, dtype=np.float64),
    }


def display_plane_bounds(shape):
    if not shape:
        return [], []
    blc = [0] * len(shape)
    trc = [max(0, int(size) - 1) for size in shape]
    for axis in range(2, len(shape)):
        center = max(0, int(shape[axis]) // 2)
        blc[axis] = center
        trc[axis] = center
    return blc, trc


def stride_for(shape, max_elements):
    if max_elements < 1:
        raise ValueError("max_elements_per_product must be >= 1")
    stride = [1] * len(shape)
    sampled = product(shape)
    if len(stride) >= 2:
        while sampled > max_elements and (shape[0] > stride[0] or shape[1] > stride[1]):
            stride[0] += 1
            stride[1] += 1
            sampled = product(math.ceil(size / step) for size, step in zip(shape, stride))
    index = 2 if len(stride) > 2 else 0
    while sampled > max_elements:
        stride[index % len(stride)] += 1
        sampled = product(math.ceil(size / step) for size, step in zip(shape, stride))
        index += 1
    return stride


def product(values):
    result = 1
    for value in values:
        result *= int(value)
    return result


def rms(values):
    return float(np.sqrt(np.nanmean(values * values)))


def finite_float(value):
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


if __name__ == "__main__":
    main()
