#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
import json
import itertools
import hashlib
import math
import os
import pathlib
import sys
import tempfile

import numpy as np


# Full-native structure analysis must remain comfortably below the resident
# size of a VLASS plane.  Every temporary ndarray created by the helpers below
# is derived from this budget; exact full-plane intermediates live in the
# request-owned disk workspace instead.
STRUCTURE_WORKING_BYTES = 16 * 1024 * 1024


# Keep this module importable by the ordinary Python test runner.  The
# comparator process resolves casatools only when it actually opens an image.
image = None

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
        request = normalized_request(json.load(handle))
    products = {}
    os.makedirs(request["panel_dir"], exist_ok=True)
    max_elements = int(request["max_elements_per_product"])
    left_prefix = request["left_prefix"]
    right_prefix = request["right_prefix"]
    expected_products = request["products"]
    structure_workspace = pathlib.Path(request["structure_workspace_dir"])
    if request["mode"] == "full":
        structure_workspace.parent.mkdir(parents=True, exist_ok=True)
        structure_workspace.mkdir()
    inventory = compare_product_inventory(
        left_prefix,
        right_prefix,
        expected_products,
        required=request["require_exact_product_inventory"],
    )
    beam_suffix = psf_beam_suffix(expected_products)
    if request["mode"] == "full":
        beam_info = estimate_native_beam_info(
            right_prefix + beam_suffix, request["full_chunk_elements"]
        )
    else:
        beam_info = estimate_beam_info(right_prefix + beam_suffix, max_elements)
    panel_displays = product_panel_displays(request, max_elements)
    try:
        for suffix in expected_products:
            left_path = left_prefix + suffix
            right_path = right_prefix + suffix
            products[suffix] = compare_one(
                left_path,
                right_path,
                max_elements,
                request["panel_dir"],
                suffix,
                beam_info,
                panel_displays.get(suffix),
                mode=request["mode"],
                full_chunk_elements=request["full_chunk_elements"],
                require_metadata_parity=request["require_metadata_parity"],
                source_regions=[
                    region
                    for region in request["source_regions"]
                    if suffix in region["products"]
                ],
                left_label=request["left_label"],
                right_label=request["right_label"],
                legacy_operand_aliases=request["legacy_operand_aliases"],
                structure_workspace_dir=request["structure_workspace_dir"],
            )
    except Exception as error:
        if request["mode"] == "full":
            write_structure_workspace_failure(
                structure_workspace, f"{type(error).__name__}: {error}"
            )
        raise
    failures = []
    if inventory["status"] == "mismatch":
        failures.append("exact product inventory differs")
    product_failures = [
        suffix
        for suffix, result in products.items()
        if result.get("status") != "compared"
    ]
    if product_failures:
        failures.append("product comparison failed for " + ", ".join(product_failures))
    output = {
        "schema_version": 4,
        "request_binding": request["request_binding"],
        "request_sha256": request["request_sha256"],
        "status": "completed" if not failures else "comparison_failed",
        "reason": "; ".join(failures) if failures else None,
        "comparison_mode": request["mode"],
        "max_elements_per_product": request["max_elements_per_product"],
        "full_chunk_elements": request["full_chunk_elements"],
        "left_prefix": left_prefix,
        "right_prefix": right_prefix,
        "left_label": request["left_label"],
        "right_label": request["right_label"],
        "requested_products": request["products"],
        "require_exact_product_inventory": request["require_exact_product_inventory"],
        "require_metadata_parity": request["require_metadata_parity"],
        "legacy_operand_aliases": request["legacy_operand_aliases"],
        "source_regions": request["source_regions"],
        "tolerances": request.get("tolerances"),
        "panel_dir": request["panel_dir"],
        "structure_workspace_dir": request["structure_workspace_dir"],
        "product_inventory": inventory,
        "beam_info": beam_info,
        "products": products,
        "structured_difference_review": summarize_product_reviews(products),
    }
    with open(sys.argv[2], "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2, sort_keys=True)
        handle.write("\n")
    if request["mode"] == "full":
        if failures:
            write_structure_workspace_failure(
                structure_workspace,
                "; ".join(failures),
            )
        # Successful product workspaces remain intact until the host validates
        # the bound output and applies its frozen tolerance contract.  The host
        # alone removes them after the complete comparison is accepted.


def normalized_request(request):
    if not isinstance(request, dict):
        raise ValueError("image comparator request must be an object")
    expected_fields = {
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
        "request_binding",
        "request_sha256",
    }
    if set(request) != expected_fields:
        missing = sorted(expected_fields - set(request))
        unknown = sorted(set(request) - expected_fields)
        raise ValueError(
            f"image comparator request field mismatch; missing={missing or 'none'}; "
            f"unknown={unknown or 'none'}"
        )
    if request.get("schema_version") != 4:
        raise ValueError("image comparator schema_version must be 4")
    mode = request.get("mode", "sampled")
    if mode not in {"sampled", "full"}:
        raise ValueError("image comparison mode must be sampled or full")
    left_prefix = request.get("left_prefix")
    right_prefix = request.get("right_prefix")
    if not isinstance(left_prefix, str) or not left_prefix:
        raise ValueError("image comparator requires left_prefix or rust_prefix")
    if not isinstance(right_prefix, str) or not right_prefix:
        raise ValueError("image comparator requires right_prefix or casa_prefix")
    max_elements = request.get("max_elements_per_product")
    full_chunk_elements = request.get("full_chunk_elements")
    if isinstance(max_elements, bool) or not isinstance(max_elements, int):
        raise ValueError("max_elements_per_product must be an integer")
    if isinstance(full_chunk_elements, bool) or not isinstance(
        full_chunk_elements, int
    ):
        raise ValueError("full_chunk_elements must be an integer")
    if max_elements < 1:
        raise ValueError("max_elements_per_product must be >= 1")
    if full_chunk_elements < 1:
        raise ValueError("full_chunk_elements must be >= 1")
    products = request.get("products")
    if (
        not isinstance(products, list)
        or not products
        or not all(
            isinstance(value, str) and value.startswith(".") for value in products
        )
    ):
        raise ValueError("image comparator products must be a non-empty suffix list")
    for key in ("require_exact_product_inventory", "require_metadata_parity"):
        if not isinstance(request.get(key), bool):
            raise ValueError(f"image comparator {key} must be a boolean")
    if not isinstance(request.get("legacy_operand_aliases"), bool):
        raise ValueError("image comparator legacy_operand_aliases must be a boolean")
    if not isinstance(request.get("panel_dir"), str) or not request["panel_dir"]:
        raise ValueError("image comparator panel_dir must be a non-empty string")
    workspace = request.get("structure_workspace_dir")
    if not isinstance(workspace, str) or not os.path.isabs(workspace):
        raise ValueError(
            "image comparator structure_workspace_dir must be an absolute path"
        )
    for key in ("left_label", "right_label"):
        if not isinstance(request.get(key), str) or not request[key]:
            raise ValueError(f"image comparator {key} must be a non-empty string")
    normalized = dict(request)
    source_regions = normalized_source_regions(
        request.get("source_regions", []), products
    )
    normalized.update(
        {
            "mode": mode,
            "left_prefix": left_prefix,
            "right_prefix": right_prefix,
            "left_label": request["left_label"],
            "right_label": request["right_label"],
            "max_elements_per_product": max_elements,
            "full_chunk_elements": full_chunk_elements,
            "require_exact_product_inventory": request[
                "require_exact_product_inventory"
            ],
            "require_metadata_parity": request["require_metadata_parity"],
            "legacy_operand_aliases": request["legacy_operand_aliases"],
            "source_regions": source_regions,
        }
    )
    binding = comparison_request_binding(normalized)
    if request.get("request_binding") != binding:
        raise ValueError(
            "image comparator request_binding does not match normalized request"
        )
    digest = canonical_sha256(binding)
    if request.get("request_sha256") != digest:
        raise ValueError(
            "image comparator request_sha256 does not match normalized request"
        )
    normalized["request_binding"] = binding
    normalized["request_sha256"] = digest
    return normalized


def comparison_request_binding(request):
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
    return {field: request[field] for field in fields}


def canonical_sha256(value):
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def structure_product_workspace(workspace_root, suffix):
    safe_suffix = suffix.strip(".").replace(".", "_") or "image"
    digest = hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:12]
    return pathlib.Path(workspace_root) / f"{safe_suffix}-{digest}"


def write_structure_workspace_failure(workspace, reason):
    workspace = pathlib.Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "failure.json").write_text(
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


def normalized_source_regions(regions, products):
    if not isinstance(regions, list):
        raise ValueError("image comparator source_regions must be a list")
    normalized = []
    seen = set()
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
            or not all(
                isinstance(suffix, str) and suffix in products for suffix in suffixes
            )
        ):
            raise ValueError(
                "source region products must name compared product suffixes"
            )
        blc = region.get("blc")
        trc = region.get("trc")
        if (
            not isinstance(blc, list)
            or not isinstance(trc, list)
            or len(blc) != 2
            or len(trc) != 2
            or not all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in [*blc, *trc]
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


def discover_product_inventory(prefix):
    prefix_path = pathlib.Path(prefix)
    parent = prefix_path.parent
    stem = prefix_path.name
    if not parent.is_dir():
        return []
    return sorted(
        path.name[len(stem) :]
        for path in parent.iterdir()
        if path.name.startswith(stem)
        and path.name != stem
        and path.name[len(stem) :].startswith(".")
    )


def compare_product_inventory(left_prefix, right_prefix, expected, required):
    expected = sorted(set(expected))
    left = discover_product_inventory(left_prefix)
    right = discover_product_inventory(right_prefix)
    left_missing = sorted(set(expected) - set(left))
    left_extra = sorted(set(left) - set(expected))
    right_missing = sorted(set(expected) - set(right))
    right_extra = sorted(set(right) - set(expected))
    mismatch = bool(left_missing or left_extra or right_missing or right_extra)
    status = "mismatch" if required and mismatch else "matched"
    if mismatch and not required:
        status = "not_required"
    return {
        "status": status,
        "required": bool(required),
        "observed_match": not mismatch,
        "expected": expected,
        "left": left,
        "right": right,
        "left_missing": left_missing,
        "left_extra": left_extra,
        "right_missing": right_missing,
        "right_extra": right_extra,
        "left_right_equal": left == right,
    }


def psf_beam_suffix(products):
    if ".psf.tt0" in products:
        return ".psf.tt0"
    return ".psf"


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
        label: [
            suffix
            for suffix, product_label in product_labels.items()
            if product_label == label
        ]
        for label in (
            "bad",
            "unknown",
            "investigate",
            "good",
            "not_applicable_exact_zero",
        )
    }
    parts = []
    for label in (
        "bad",
        "unknown",
        "investigate",
        "good",
        "not_applicable_exact_zero",
    ):
        suffixes = grouped[label]
        if suffixes:
            parts.append(f"{label}: {', '.join(suffixes)}")
    return f"overall {overall}; " + "; ".join(parts)


def product_panel_displays(request, max_elements):
    displays = {}
    left_prefix = request.get("left_prefix", request.get("rust_prefix"))
    right_prefix = request.get("right_prefix", request.get("casa_prefix"))
    for suffix in request["products"]:
        if model_restoration_suffixes(suffix) is None:
            continue
        restored = restored_model_panel_display(
            left_prefix=left_prefix,
            right_prefix=right_prefix,
            model_suffix=suffix,
            max_elements=max_elements,
        )
        if restored is not None:
            displays[suffix] = restored
    return displays


def model_restoration_suffixes(model_suffix):
    if model_suffix == ".model":
        return ".image", ".residual"
    if model_suffix.startswith(".model.tt") and model_suffix[9:].isdigit():
        taylor_suffix = model_suffix[len(".model") :]
        return ".image" + taylor_suffix, ".residual" + taylor_suffix
    return None


def restored_model_panel_display(
    left_prefix=None,
    right_prefix=None,
    model_suffix=".model",
    max_elements=1_000_000,
    rust_prefix=None,
    casa_prefix=None,
):
    left_prefix = left_prefix or rust_prefix
    right_prefix = right_prefix or casa_prefix
    family = model_restoration_suffixes(model_suffix)
    if family is None:
        return None
    image_suffix, residual_suffix = family
    required = {
        "left_image": left_prefix + image_suffix,
        "left_residual": left_prefix + residual_suffix,
        "right_image": right_prefix + image_suffix,
        "right_residual": right_prefix + residual_suffix,
    }
    missing = [path for path in required.values() if not os.path.isdir(path)]
    if missing:
        return {
            "status": "unavailable",
            "reason": "restored model visualization requires .image and .residual",
            "missing_paths": missing,
        }
    try:
        left_image = load_image_display_plane(required["left_image"], max_elements)
        left_residual = load_image_display_plane(
            required["left_residual"], max_elements
        )
        right_image = load_image_display_plane(required["right_image"], max_elements)
        right_residual = load_image_display_plane(
            required["right_residual"], max_elements
        )
    except Exception as error:
        return {
            "status": "unavailable",
            "reason": f"failed to load restored model visualization inputs: {error}",
        }
    inputs = [left_image, left_residual, right_image, right_residual]
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
    left_display = left_image["data"] - left_residual["data"]
    right_display = right_image["data"] - right_residual["data"]
    return {
        "status": "available",
        "left_data": left_display,
        "right_data": right_display,
        "rust_data": left_display,
        "casa_data": right_display,
        "diff_data": left_display - right_display,
        "transform": "restored_model_from_image_minus_residual",
        "description": (
            f"{model_suffix} visualized as restoring-beam-convolved model via "
            f"{image_suffix} - {residual_suffix}"
        ),
        "product_label": f"{model_suffix} restored",
        "value_label": "Jy/beam",
        "shape": shapes[0],
        "sample_stride": strides[0],
    }


def compare_one(
    rust_path,
    casa_path,
    max_elements,
    panel_dir,
    suffix,
    beam_info,
    panel_display=None,
    mode="sampled",
    full_chunk_elements=1_000_000,
    require_metadata_parity=False,
    source_regions=None,
    left_label="casa-rs",
    right_label="CASA",
    legacy_operand_aliases=True,
    structure_workspace_dir=None,
):
    left_path = rust_path
    right_path = casa_path
    if not os.path.isdir(left_path) or not os.path.isdir(right_path):
        return {
            "status": "missing",
            "left_path": left_path,
            "right_path": right_path,
            "left_exists": os.path.isdir(left_path),
            "right_exists": os.path.isdir(right_path),
            "rust_path": left_path,
            "casa_path": right_path,
            "rust_exists": os.path.isdir(left_path),
            "casa_exists": os.path.isdir(right_path),
        }
    left = load_image(left_path, max_elements)
    right = load_image(right_path, max_elements)
    if left["shape"] != right["shape"]:
        return {
            "status": "shape_mismatch",
            "left_path": left_path,
            "right_path": right_path,
            "left_shape": left["shape"],
            "right_shape": right["shape"],
            "rust_path": left_path,
            "casa_path": right_path,
            "rust_shape": left["shape"],
            "casa_shape": right["shape"],
        }
    left_data = left["data"]
    right_data = right["data"]
    mask = np.isfinite(left_data) & np.isfinite(right_data)
    valid_count = int(np.count_nonzero(mask))
    if valid_count == 0 and mode != "full":
        return {
            "status": "no_finite_overlap",
            "left_path": left_path,
            "right_path": right_path,
            "rust_path": left_path,
            "casa_path": right_path,
            "shape": left["shape"],
            "sample_stride": left["sample_stride"],
            "sampled_elements": int(left_data.size),
        }
    diff_full = left_data - right_data
    left_peak = peak_summary(left_data)
    right_peak_summary = peak_summary(right_data)
    diff_peak = peak_summary(diff_full)
    if valid_count:
        left_valid = left_data[mask]
        right_valid = right_data[mask]
        diff = left_valid - right_valid
        right_peak = max(
            abs(float(np.nanmin(right_valid))), abs(float(np.nanmax(right_valid)))
        )
        right_rms = rms(right_valid)
        diff_rms = rms(diff)
        diff_abs_max = float(np.nanmax(np.abs(diff)))
        correlation = correlation_value(left_valid, right_valid)
        left_min = finite_float(np.nanmin(left_valid))
        left_max = finite_float(np.nanmax(left_valid))
        left_rms = finite_float(rms(left_valid))
        right_min = finite_float(np.nanmin(right_valid))
        right_max = finite_float(np.nanmax(right_valid))
    else:
        # Sampling is only the bounded panel/diagnostic path.  In full mode the
        # streamed reducer below owns the authoritative finite-overlap decision.
        right_peak = None
        right_rms = None
        diff_rms = None
        diff_abs_max = None
        correlation = None
        left_min = None
        left_max = None
        left_rms = None
        right_min = None
        right_max = None
    structure = structured_difference_metrics(
        suffix=suffix,
        rust_data=left_data,
        casa_data=right_data,
        diff_data=diff_full,
        beam_info=beam_info,
    )
    panel_display_data = panel_display
    if panel_display_data is None:
        left_display = load_image_display_plane(left_path, max_elements)
        right_display = load_image_display_plane(right_path, max_elements)
        panel_display_data = {
            "status": "available",
            "left_data": left_display["data"],
            "right_data": right_display["data"],
            "rust_data": left_display["data"],
            "casa_data": right_display["data"],
            "diff_data": left_display["data"] - right_display["data"],
            "transform": "center_plane_full_spatial_display",
            "description": (
                "center display plane loaded with spatial-only stride; "
                "non-spatial axes fixed at their center"
            ),
            "shape": left_display["shape"],
            "display_bounds": left_display["display_bounds"],
            "sample_stride": left_display["sample_stride"],
        }
    panel_arguments = {
        "panel_dir": panel_dir,
        "suffix": suffix,
        "rust_data": left_data,
        "casa_data": right_data,
        "diff_data": diff_full,
        "review": structure.get("review") if isinstance(structure, dict) else None,
        "display": panel_display_data,
    }
    if left_label != "casa-rs" or right_label != "CASA":
        panel_arguments["left_label"] = left_label
        panel_arguments["right_label"] = right_label
    panel = write_review_panel(**panel_arguments)

    metadata = (
        compare_image_metadata(left_path, right_path)
        if require_metadata_parity
        else {"status": "not_required", "parity": None}
    )
    metadata_mismatch = require_metadata_parity and metadata["status"] != "matched"
    result = {
        "status": "metadata_mismatch" if metadata_mismatch else "compared",
        "comparison_mode": mode,
        "left_label": left_label,
        "right_label": right_label,
        "left_path": left_path,
        "right_path": right_path,
        "rust_path": left_path,
        "casa_path": right_path,
        "shape": left["shape"],
        "sample_stride": left["sample_stride"],
        "sampled_elements": int(left_data.size),
        "finite_overlap": valid_count,
        "left_min": left_min,
        "left_max": left_max,
        "left_rms": left_rms,
        "right_min": right_min,
        "right_max": right_max,
        "right_rms": finite_float(right_rms),
        "diff_abs_max": finite_float(diff_abs_max),
        "diff_rms": finite_float(diff_rms),
        "diff_rms_over_right_rms": relative_difference_ratio(diff_rms, right_rms),
        "diff_abs_max_over_right_peak": relative_difference_ratio(
            diff_abs_max, right_peak
        ),
        "correlation": finite_float(correlation) if correlation is not None else None,
        "left_peak_abs": left_peak,
        "right_peak_abs": right_peak_summary,
        "diff_peak_abs": diff_peak,
        "metadata": metadata,
        "metadata_parity_required": bool(require_metadata_parity),
        "structured_difference": structure,
        "review_panel": panel,
    }
    # Existing ledger and report readers still consume the historical names.
    if legacy_operand_aliases:
        result.update(
            {
                "rust_min": result["left_min"],
                "rust_max": result["left_max"],
                "rust_rms": result["left_rms"],
                "casa_min": result["right_min"],
                "casa_max": result["right_max"],
                "casa_rms": result["right_rms"],
                "diff_rms_over_casa_rms": result["diff_rms_over_right_rms"],
                "diff_abs_max_over_casa_peak": result["diff_abs_max_over_right_peak"],
                "rust_peak_abs": result["left_peak_abs"],
                "casa_peak_abs": result["right_peak_abs"],
            }
        )
    if mode == "full":
        if structure_workspace_dir is None:
            product_workspace = None
        else:
            product_workspace = structure_product_workspace(
                structure_workspace_dir, suffix
            )
        full = full_array_statistics(
            left_path,
            right_path,
            full_chunk_elements,
            structure_suffix=suffix,
            structure_beam_info=beam_info,
            structure_scratch_root=product_workspace,
            defer_structure_cleanup=product_workspace is not None,
        )
        result["full_array"] = full
        if isinstance(full.get("structured_difference"), dict):
            result["sampled_structured_difference"] = result["structured_difference"]
            result["structured_difference"] = full["structured_difference"]
            panel_arguments["review"] = full["structured_difference"].get("review")
            result["review_panel"] = write_review_panel(**panel_arguments)
        if full.get("status") == "compared":
            result.update(
                {
                    "left_min": full["left"]["min"],
                    "left_max": full["left"]["max"],
                    "left_rms": full["left"]["rms"],
                    "right_min": full["right"]["min"],
                    "right_max": full["right"]["max"],
                    "right_rms": full["right"]["rms"],
                    "left_peak_abs": full["left"]["peak_abs"],
                    "right_peak_abs": full["right"]["peak_abs"],
                    "diff_peak_abs": full["difference"]["peak_abs"],
                }
            )
            for name in (
                "diff_abs_max",
                "diff_rms",
                "diff_rms_over_right_rms",
                "diff_abs_max_over_right_peak",
                "correlation",
            ):
                result[name] = full.get(name)
            result["finite_overlap"] = full.get("comparison_domain_count")
            topology = full.get("topology", {})
            result["topology_parity"] = bool(
                topology.get("mask_equal")
                and topology.get("finite_equal")
                and topology.get("nonfinite_kind_equal")
            )
            if not result["topology_parity"]:
                result["status"] = "topology_mismatch"
            if not full.get("coverage_complete"):
                result["status"] = "full_coverage_incomplete"
            if legacy_operand_aliases:
                result.update(
                    {
                        "rust_min": result["left_min"],
                        "rust_max": result["left_max"],
                        "rust_rms": result["left_rms"],
                        "casa_min": result["right_min"],
                        "casa_max": result["right_max"],
                        "casa_rms": result["right_rms"],
                        "rust_peak_abs": result["left_peak_abs"],
                        "casa_peak_abs": result["right_peak_abs"],
                    }
                )
                result["diff_rms_over_casa_rms"] = result["diff_rms_over_right_rms"]
                result["diff_abs_max_over_casa_peak"] = result[
                    "diff_abs_max_over_right_peak"
                ]
        else:
            result["status"] = full.get("status", "full_comparison_failed")
        if source_regions:
            try:
                result["source_regions"] = compare_source_regions(
                    left_path,
                    right_path,
                    source_regions,
                    max_elements=full_chunk_elements,
                    left_beam_area_pixels=metadata_beam_area_pixels(
                        metadata.get("left")
                    ),
                    right_beam_area_pixels=metadata_beam_area_pixels(
                        metadata.get("right")
                    ),
                )
            except ValueError as error:
                result["source_regions"] = []
                result["source_region_failure"] = str(error)
                result["status"] = "source_region_failed"
    return result


def compare_source_regions(
    left_path,
    right_path,
    regions,
    max_elements,
    image_factory=None,
    left_beam_area_pixels=None,
    right_beam_area_pixels=None,
):
    """Stream bounded source boxes and report source-local flux/centroid facts.

    For Jy/beam images, integrated flux is the finite, unmasked pixel sum inside
    the frozen box divided by that image's Gaussian restoring-beam area in
    pixels.  Centroids use absolute pixel values as non-negative weights and
    are reported in zero-based pixel coordinates.
    """

    return [
        {
            **region,
            "method": (
                "finite_unmasked_region_sum_over_restoring_beam_area_"
                "and_abs_weighted_centroid"
            ),
            "left": source_region_statistics(
                left_path,
                region["blc"],
                region["trc"],
                max_elements=max_elements,
                image_factory=image_factory,
                beam_area_pixels=left_beam_area_pixels,
            ),
            "right": source_region_statistics(
                right_path,
                region["blc"],
                region["trc"],
                max_elements=max_elements,
                image_factory=image_factory,
                beam_area_pixels=right_beam_area_pixels,
            ),
        }
        for region in regions
    ]


def source_region_statistics(
    path,
    blc_xy,
    trc_xy,
    max_elements,
    image_factory=None,
    beam_area_pixels=None,
):
    if max_elements < 1:
        raise ValueError("source-region chunk budget must be >= 1")
    tool = new_image_tool(image_factory)
    tool.open(path)
    try:
        shape = [int(value) for value in tool.shape()]
        if len(shape) < 2:
            raise ValueError(f"source region requires at least two image axes: {path}")
        if trc_xy[0] >= shape[0] or trc_xy[1] >= shape[1]:
            raise ValueError(
                f"source region is outside image shape {shape}: {blc_xy}..{trc_xy}"
            )
        other_axes = [0] * (len(shape) - 2)
        y_count = trc_xy[1] - blc_xy[1] + 1
        y_chunk = min(y_count, max_elements)
        x_chunk = max(1, max_elements // y_chunk)
        count = 0
        value_sum = math.fsum([])
        weight_sum = math.fsum([])
        weighted_x = math.fsum([])
        weighted_y = math.fsum([])
        peak = None
        chunks = 0
        for x_start in range(blc_xy[0], trc_xy[0] + 1, x_chunk):
            x_end = min(trc_xy[0], x_start + x_chunk - 1)
            for y_start in range(blc_xy[1], trc_xy[1] + 1, y_chunk):
                y_end = min(trc_xy[1], y_start + y_chunk - 1)
                blc = [x_start, y_start, *other_axes]
                trc = [x_end, y_end, *other_axes]
                inc = [1] * len(shape)
                data = np.asarray(
                    tool.getchunk(
                        blc=blc,
                        trc=trc,
                        inc=inc,
                        dropdeg=False,
                        getmask=False,
                    )
                )
                pixel_mask = np.asarray(
                    tool.getchunk(
                        blc=blc,
                        trc=trc,
                        inc=inc,
                        dropdeg=False,
                        getmask=True,
                    ),
                    dtype=bool,
                )
                valid = pixel_mask & np.isfinite(data)
                if np.any(valid):
                    values = np.asarray(data[valid], dtype=np.float64)
                    coordinates = np.nonzero(valid)
                    xs = np.asarray(coordinates[0], dtype=np.float64) + x_start
                    ys = np.asarray(coordinates[1], dtype=np.float64) + y_start
                    weights = np.abs(values)
                    count += int(values.size)
                    value_sum += math.fsum(float(value) for value in values)
                    weight_sum += math.fsum(float(value) for value in weights)
                    weighted_x += math.fsum(float(value) for value in weights * xs)
                    weighted_y += math.fsum(float(value) for value in weights * ys)
                    local = int(np.argmax(weights))
                    candidate = {
                        "location": [int(xs[local]), int(ys[local])],
                        "value": finite_float(values[local]),
                        "abs_value": finite_float(weights[local]),
                    }
                    if peak is None or candidate["abs_value"] > peak["abs_value"]:
                        peak = candidate
                chunks += 1
        centroid = (
            [
                finite_float(weighted_x / weight_sum),
                finite_float(weighted_y / weight_sum),
            ]
            if weight_sum
            else None
        )
        return {
            "status": "measured" if count else "no_finite_pixels",
            "finite_unmasked_count": count,
            "integrated_pixel_sum": finite_float(value_sum),
            "beam_area_pixels": finite_float(beam_area_pixels)
            if beam_area_pixels is not None
            else None,
            "integrated_flux": finite_float(value_sum / beam_area_pixels)
            if beam_area_pixels is not None and beam_area_pixels > 0.0
            else None,
            "centroid_pixels": centroid,
            "peak_abs": peak,
            "chunks": chunks,
            "max_chunk_elements": max_elements,
        }
    finally:
        tool.close()


def metadata_beam_area_pixels(metadata):
    """Return Gaussian restoring-beam area in direction pixels, if measurable."""

    if not isinstance(metadata, dict):
        return None
    beam = metadata.get("restoring_beam")
    coordinates = metadata.get("coordinates")
    if not isinstance(beam, dict) or not isinstance(coordinates, dict):
        return None
    direction = next(
        (
            value
            for key, value in coordinates.items()
            if str(key).startswith("direction") and isinstance(value, dict)
        ),
        None,
    )
    if not isinstance(direction, dict):
        return None
    increments = direction.get("cdelt")
    units = direction.get("units")
    if (
        not isinstance(increments, list)
        or len(increments) < 2
        or not isinstance(units, list)
        or len(units) < 2
    ):
        return None
    major = angular_quantity_radians(beam.get("major"))
    minor = angular_quantity_radians(beam.get("minor"))
    x_increment = angular_value_radians(increments[0], units[0])
    y_increment = angular_value_radians(increments[1], units[1])
    if not all(
        value is not None and math.isfinite(value) and value != 0.0
        for value in (major, minor, x_increment, y_increment)
    ):
        return None
    return (
        math.pi
        / (4.0 * math.log(2.0))
        * abs(major * minor)
        / abs(x_increment * y_increment)
    )


def angular_quantity_radians(quantity):
    if not isinstance(quantity, dict):
        return None
    return angular_value_radians(quantity.get("value"), quantity.get("unit"))


def angular_value_radians(value, unit):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    scales = {
        "rad": 1.0,
        "deg": math.pi / 180.0,
        "arcmin": math.pi / (180.0 * 60.0),
        "arcsec": math.pi / (180.0 * 3600.0),
    }
    scale = scales.get(unit)
    return float(value) * scale if scale is not None else None


def new_image_tool(image_factory=None):
    factory = image_factory
    if factory is None:
        global image
        if image is None:
            from casatools import image as casa_image

            image = casa_image
        factory = image
    return factory()


def normalize_serializable(value):
    if isinstance(value, dict):
        return {
            str(key): normalize_serializable(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key).lower() != "parentname"
        }
    if isinstance(value, (list, tuple)):
        return [normalize_serializable(item) for item in value]
    if isinstance(value, np.ndarray):
        return normalize_serializable(value.tolist())
    if isinstance(value, np.generic):
        return normalize_serializable(value.item())
    if isinstance(value, complex):
        return {
            "real": finite_json_number(value.real),
            "imag": finite_json_number(value.imag),
        }
    if isinstance(value, float):
        return finite_json_number(value)
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def finite_json_number(value):
    value = float(value)
    if math.isnan(value):
        return "NaN"
    if value == math.inf:
        return "+Infinity"
    if value == -math.inf:
        return "-Infinity"
    return value


def image_metadata(path, image_factory=None):
    tool = new_image_tool(image_factory)
    coordinate_tool = None
    errors = []
    try:
        tool.open(path)
        shape = [int(value) for value in tool.shape()]
        try:
            unit = normalize_serializable(tool.brightnessunit())
        except Exception as error:
            unit = None
            errors.append(f"brightnessunit: {error}")
        try:
            coordinate_tool = tool.coordsys()
            coordinates = normalize_serializable(coordinate_tool.torecord())
        except Exception as error:
            coordinates = None
            errors.append(f"coordinates: {error}")
        try:
            restoring_beam = normalize_serializable(tool.restoringbeam())
        except Exception as error:
            restoring_beam = None
            errors.append(f"restoringbeam: {error}")
        try:
            masks = normalize_serializable(tool.maskhandler("get"))
            if isinstance(masks, list) and all(
                isinstance(value, str) for value in masks
            ):
                masks = sorted(masks)
        except Exception as error:
            masks = None
            errors.append(f"maskhandler: {error}")
    finally:
        if coordinate_tool is not None:
            closer = getattr(coordinate_tool, "done", None) or getattr(
                coordinate_tool, "close", None
            )
            if closer is not None:
                closer()
        tool.close()
    return {
        "status": "complete" if not errors else "incomplete",
        "shape": shape,
        "unit": unit,
        "coordinates": coordinates,
        "restoring_beam": restoring_beam,
        "masks": masks,
        "errors": errors,
    }


def compare_image_metadata(left_path, right_path, image_factory=None):
    try:
        left = image_metadata(left_path, image_factory=image_factory)
        right = image_metadata(right_path, image_factory=image_factory)
    except Exception as error:
        return {
            "status": "unavailable",
            "reason": str(error),
            "parity": False,
        }
    fields = ("shape", "unit", "coordinates", "restoring_beam", "masks")
    parity = {name: left.get(name) == right.get(name) for name in fields}
    complete = left["status"] == "complete" and right["status"] == "complete"
    return {
        "status": "matched" if complete and all(parity.values()) else "mismatch",
        "parity": complete and all(parity.values()),
        "field_parity": parity,
        "left": left,
        "right": right,
    }


def full_chunk_shape(shape, max_elements):
    if max_elements < 1:
        raise ValueError("full_chunk_elements must be >= 1")
    if not shape:
        return []
    result = []
    capacity = int(max_elements)
    for size in shape:
        extent = min(int(size), max(1, capacity))
        result.append(extent)
        capacity = max(1, capacity // max(1, extent))
    return result


def iter_full_chunks(shape, max_elements):
    if any(int(size) <= 0 for size in shape):
        return
    chunk_shape = full_chunk_shape(shape, max_elements)
    starts = [range(0, int(size), extent) for size, extent in zip(shape, chunk_shape)]
    # CASA image axis zero is the fastest-varying axis.  Iterate it innermost
    # so adjacent rectangular chunks remain sequential in table/lattice order.
    for reversed_values in itertools.product(*reversed(starts)):
        blc = [int(value) for value in reversed(reversed_values)]
        trc = [
            min(int(size), start + extent) - 1
            for size, start, extent in zip(shape, blc, chunk_shape)
        ]
        elements = product(end - start + 1 for start, end in zip(blc, trc))
        if elements > max_elements:
            raise RuntimeError("internal full-array chunk exceeds full_chunk_elements")
        yield blc, trc, elements


class CompensatedSum:
    def __init__(self):
        self.total = 0.0
        self.correction = 0.0

    def add(self, value):
        value = float(value)
        updated = self.total + value
        if abs(self.total) >= abs(value):
            self.correction += (self.total - updated) + value
        else:
            self.correction += (value - updated) + self.total
        self.total = updated

    def value(self):
        return self.total + self.correction


class FullSpatialStructureReducer:
    """Stage the exact native central plane in bounded-memory disk arrays."""

    def __init__(self, shape, scratch_root=None):
        self.shape = [int(value) for value in shape]
        self.available = len(self.shape) >= 2
        self.spatial_pixels_visited = 0
        self.write_chunks = 0
        self.left_raw_finite_pixels = 0
        self.right_raw_finite_pixels = 0
        self.paired_raw_finite_pixels = 0
        self.paired_raw_left_abs_max = 0.0
        self.paired_raw_right_abs_max = 0.0
        self.paired_raw_diff_abs_max = 0.0
        self.paired_image_mask_finite_pixels = 0
        self.central_mask_mismatch_pixels = 0
        self.overlap_write_pixels = 0
        self._temporary_directory = None
        self.workspace_path = None
        self.workspace_external = scratch_root is not None
        if not self.available:
            return
        if scratch_root is None:
            self._temporary_directory = tempfile.TemporaryDirectory(
                prefix=".full-native-structure-"
            )
            root = pathlib.Path(self._temporary_directory.name)
        else:
            root = pathlib.Path(scratch_root)
            root.parent.mkdir(parents=True, exist_ok=True)
            root.mkdir()
        self.workspace_path = root
        native_shape = tuple(self.shape[:2])
        self.left_native = np.memmap(
            root / "left.f64", mode="w+", dtype=np.float64, shape=native_shape
        )
        self.right_native = np.memmap(
            root / "right.f64", mode="w+", dtype=np.float64, shape=native_shape
        )
        self.diff_native = np.memmap(
            root / "diff.f64", mode="w+", dtype=np.float64, shape=native_shape
        )
        self.coverage = np.memmap(
            root / "coverage.u8", mode="w+", dtype=np.uint8, shape=native_shape
        )
        self.coverage[:] = 0

    def add(self, left, right, left_mask, right_mask, blc):
        if not self.available:
            return
        selection = [slice(None), slice(None)]
        for axis in range(2, len(self.shape)):
            center = self.shape[axis] // 2
            offset = center - int(blc[axis])
            if offset < 0 or offset >= left.shape[axis]:
                return
            selection.append(offset)
        selection_tuple = tuple(selection)
        left_plane = np.asarray(left[selection_tuple], dtype=np.float64)
        right_plane = np.asarray(right[selection_tuple], dtype=np.float64)
        left_plane_mask = np.asarray(left_mask[selection_tuple], dtype=bool)
        right_plane_mask = np.asarray(right_mask[selection_tuple], dtype=bool)
        if left_plane.ndim != 2 or right_plane.shape != left_plane.shape:
            raise ValueError("central spatial structure chunk is not two-dimensional")
        target = (
            slice(int(blc[0]), int(blc[0]) + left_plane.shape[0]),
            slice(int(blc[1]), int(blc[1]) + left_plane.shape[1]),
        )
        existing_coverage = self.coverage[target]
        self.overlap_write_pixels += int(np.count_nonzero(existing_coverage))
        self.coverage[target] = 1
        self.left_native[target] = left_plane
        self.right_native[target] = right_plane
        with np.errstate(invalid="ignore", over="ignore"):
            self.diff_native[target] = left_plane - right_plane
        self.spatial_pixels_visited += int(left_plane.size)
        self.write_chunks += 1
        left_finite = np.isfinite(left_plane)
        right_finite = np.isfinite(right_plane)
        paired_raw_finite = left_finite & right_finite
        self.left_raw_finite_pixels += int(np.count_nonzero(left_finite))
        self.right_raw_finite_pixels += int(np.count_nonzero(right_finite))
        self.paired_raw_finite_pixels += int(np.count_nonzero(paired_raw_finite))
        if np.any(paired_raw_finite):
            paired_left = left_plane[paired_raw_finite]
            paired_right = right_plane[paired_raw_finite]
            self.paired_raw_left_abs_max = max(
                self.paired_raw_left_abs_max,
                float(np.max(np.abs(paired_left))),
            )
            self.paired_raw_right_abs_max = max(
                self.paired_raw_right_abs_max,
                float(np.max(np.abs(paired_right))),
            )
            self.paired_raw_diff_abs_max = max(
                self.paired_raw_diff_abs_max,
                float(np.max(np.abs(paired_left - paired_right))),
            )
        self.paired_image_mask_finite_pixels += int(
            np.count_nonzero(paired_raw_finite & left_plane_mask & right_plane_mask)
        )
        self.central_mask_mismatch_pixels += int(
            np.count_nonzero(left_plane_mask != right_plane_mask)
        )

    def metrics(self, suffix, beam_info):
        if not self.available:
            return {
                "status": "unavailable",
                "reason": "image has fewer than two spatial axes",
            }
        expected_pixels = self.shape[0] * self.shape[1]
        covered_pixels = int(np.count_nonzero(self.coverage))
        coverage_complete = bool(
            covered_pixels == expected_pixels
            and self.spatial_pixels_visited == expected_pixels
            and self.overlap_write_pixels == 0
        )
        reduction = {
            "source_shape": self.shape[:2],
            "storage": "temporary_disk_backed_native_arrays",
            "method": "exact_native_central_plane_disk_backed_memmap",
            "array_count": 4,
            "temporary_bytes": expected_pixels * (8 * 3 + 1),
            "spatial_pixels_visited": self.spatial_pixels_visited,
            "covered_pixels": covered_pixels,
            "expected_pixels": expected_pixels,
            "overlap_write_pixels": self.overlap_write_pixels,
            "coverage_complete": coverage_complete,
            "write_chunks": self.write_chunks,
            "structure_value_domain": (
                "raw_paired_finite_stored_values_before_image_mask_application"
            ),
            "left_raw_finite_pixels": self.left_raw_finite_pixels,
            "right_raw_finite_pixels": self.right_raw_finite_pixels,
            "paired_raw_finite_pixels": self.paired_raw_finite_pixels,
            "paired_image_mask_finite_pixels": (self.paired_image_mask_finite_pixels),
            "central_mask_mismatch_pixels": self.central_mask_mismatch_pixels,
            "workspace_lifecycle": "remove_on_success_retain_on_failure",
        }
        if not reduction["coverage_complete"]:
            return incomplete_structure_coverage_evidence(suffix, reduction)
        adjusted_beam = native_structure_beam_info(beam_info)
        metrics = structured_difference_metrics(
            suffix,
            self.left_native,
            self.right_native,
            self.diff_native,
            adjusted_beam,
            scratch_root=self.workspace_path,
        )
        metrics["evidence_scope"] = "full_native_central_spatial_plane_disk_backed"
        metrics["native_spatial_evidence"] = reduction
        metrics["beam_info"] = adjusted_beam
        return metrics

    def proves_exact_zero_operands(self):
        return bool(
            self.spatial_pixels_visited == self.shape[0] * self.shape[1]
            and self.overlap_write_pixels == 0
            and self.paired_raw_finite_pixels > 0
            and self.paired_raw_left_abs_max == 0.0
            and self.paired_raw_right_abs_max == 0.0
            and self.paired_raw_diff_abs_max == 0.0
        )

    def close(self, *, retain=False, failure=None):
        for name in ("left_native", "right_native", "diff_native", "coverage"):
            value = getattr(self, name, None)
            if value is not None:
                value.flush()
                delattr(self, name)
        if retain and self.workspace_external and self.workspace_path is not None:
            if failure is not None:
                failure_payload = {
                    "status": "retained_exact_structure_workspace",
                    "reason": failure,
                }
                (self.workspace_path / "failure.json").write_text(
                    json.dumps(failure_payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            return
        if self.workspace_path is not None and self.workspace_path.exists():
            for name in ("left.f64", "right.f64", "diff.f64", "coverage.u8"):
                path = self.workspace_path / name
                if path.exists():
                    path.unlink()
            self.workspace_path.rmdir()
            self.workspace_path = None
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
            self._temporary_directory = None


class FullArrayReducer:
    def __init__(
        self,
        shape,
        chunk_budget,
        *,
        structure_suffix=None,
        structure_beam_info=None,
        structure_scratch_root=None,
    ):
        self.shape = [int(value) for value in shape]
        self.chunk_budget = int(chunk_budget)
        self.total_elements = product(shape)
        self.elements_visited = 0
        self.chunks = 0
        self.max_chunk_elements_observed = 0
        self.left_masked = 0
        self.right_masked = 0
        self.mask_mismatch = 0
        self.left_finite = 0
        self.right_finite = 0
        self.finite_topology_mismatch = 0
        self.nonfinite_kind_mismatch = 0
        self.left_nan = 0
        self.right_nan = 0
        self.left_posinf = 0
        self.right_posinf = 0
        self.left_neginf = 0
        self.right_neginf = 0
        self.paired_count = 0
        self.left_min = math.inf
        self.left_max = -math.inf
        self.right_min = math.inf
        self.right_max = -math.inf
        self.left_sum = CompensatedSum()
        self.right_sum = CompensatedSum()
        self.left_sum_squares = CompensatedSum()
        self.right_sum_squares = CompensatedSum()
        self.cross_sum = CompensatedSum()
        self.diff_sum = CompensatedSum()
        self.diff_sum_squares = CompensatedSum()
        self.diff_abs_max = 0.0
        self.left_peak = None
        self.right_peak = None
        self.diff_peak = None
        self.structure_suffix = structure_suffix
        self.structure_beam_info = structure_beam_info
        self.spatial_structure = (
            FullSpatialStructureReducer(self.shape, structure_scratch_root)
            if structure_suffix is not None
            else None
        )

    def close(self, *, retain=False, failure=None):
        if self.spatial_structure is not None:
            self.spatial_structure.close(retain=retain, failure=failure)

    def add(self, left, right, left_mask, right_mask, blc):
        left = np.asarray(left, dtype=np.float64)
        right = np.asarray(right, dtype=np.float64)
        left_mask = np.asarray(left_mask, dtype=bool)
        right_mask = np.asarray(right_mask, dtype=bool)
        if left.shape != right.shape:
            raise ValueError("left/right full-array chunks have different shapes")
        if left_mask.shape != left.shape:
            left_mask = np.broadcast_to(left_mask, left.shape)
        if right_mask.shape != right.shape:
            right_mask = np.broadcast_to(right_mask, right.shape)
        if left.size > self.chunk_budget:
            raise ValueError("full-array chunk exceeds full_chunk_elements")

        self.elements_visited += int(left.size)
        self.chunks += 1
        self.max_chunk_elements_observed = max(
            self.max_chunk_elements_observed, int(left.size)
        )
        self.left_masked += int(left.size - np.count_nonzero(left_mask))
        self.right_masked += int(right.size - np.count_nonzero(right_mask))
        self.mask_mismatch += int(np.count_nonzero(left_mask != right_mask))

        left_finite = np.isfinite(left)
        right_finite = np.isfinite(right)
        topology_domain = left_mask & right_mask
        self.left_finite += int(np.count_nonzero(left_finite & topology_domain))
        self.right_finite += int(np.count_nonzero(right_finite & topology_domain))
        self.finite_topology_mismatch += int(
            np.count_nonzero((left_finite != right_finite) & topology_domain)
        )
        left_nan = np.isnan(left)
        right_nan = np.isnan(right)
        left_posinf = np.isposinf(left)
        right_posinf = np.isposinf(right)
        left_neginf = np.isneginf(left)
        right_neginf = np.isneginf(right)
        self.left_nan += int(np.count_nonzero(left_nan & topology_domain))
        self.right_nan += int(np.count_nonzero(right_nan & topology_domain))
        self.left_posinf += int(np.count_nonzero(left_posinf & topology_domain))
        self.right_posinf += int(np.count_nonzero(right_posinf & topology_domain))
        self.left_neginf += int(np.count_nonzero(left_neginf & topology_domain))
        self.right_neginf += int(np.count_nonzero(right_neginf & topology_domain))
        same_kind = (
            (left_finite & right_finite)
            | (left_nan & right_nan)
            | (left_posinf & right_posinf)
            | (left_neginf & right_neginf)
        )
        self.nonfinite_kind_mismatch += int(
            np.count_nonzero((~same_kind) & topology_domain)
        )

        valid = left_mask & right_mask & left_finite & right_finite
        if self.spatial_structure is not None:
            self.spatial_structure.add(left, right, left_mask, right_mask, blc)
        count = int(np.count_nonzero(valid))
        if count == 0:
            return
        left_values = left[valid]
        right_values = right[valid]
        diff = left_values - right_values
        self.paired_count += count
        self.left_min = min(self.left_min, float(np.min(left_values)))
        self.left_max = max(self.left_max, float(np.max(left_values)))
        self.right_min = min(self.right_min, float(np.min(right_values)))
        self.right_max = max(self.right_max, float(np.max(right_values)))
        self.left_sum.add(np.sum(left_values, dtype=np.float64))
        self.right_sum.add(np.sum(right_values, dtype=np.float64))
        self.left_sum_squares.add(np.dot(left_values, left_values))
        self.right_sum_squares.add(np.dot(right_values, right_values))
        self.cross_sum.add(np.dot(left_values, right_values))
        self.diff_sum.add(np.sum(diff, dtype=np.float64))
        self.diff_sum_squares.add(np.dot(diff, diff))
        self.diff_abs_max = max(self.diff_abs_max, float(np.max(np.abs(diff))))
        self.left_peak = update_chunk_peak(self.left_peak, left, valid, blc)
        self.right_peak = update_chunk_peak(self.right_peak, right, valid, blc)
        self.diff_peak = update_chunk_peak(self.diff_peak, left - right, valid, blc)

    def result(self):
        if self.paired_count == 0:
            status = "no_finite_overlap"
            left_rms = None
            right_rms = None
            diff_rms = None
            correlation = None
            covariance = None
        else:
            status = "compared"
            count = float(self.paired_count)
            left_sum = self.left_sum.value()
            right_sum = self.right_sum.value()
            left_squares = self.left_sum_squares.value()
            right_squares = self.right_sum_squares.value()
            cross = self.cross_sum.value()
            left_rms = math.sqrt(max(0.0, left_squares / count))
            right_rms = math.sqrt(max(0.0, right_squares / count))
            diff_rms = math.sqrt(max(0.0, self.diff_sum_squares.value() / count))
            covariance_numerator = cross - left_sum * right_sum / count
            left_variance_numerator = left_squares - left_sum * left_sum / count
            right_variance_numerator = right_squares - right_sum * right_sum / count
            covariance = covariance_numerator / count
            denominator = math.sqrt(
                max(0.0, left_variance_numerator) * max(0.0, right_variance_numerator)
            )
            correlation = covariance_numerator / denominator if denominator else None
        right_peak = self.right_peak["abs_value"] if self.right_peak else None
        result = {
            "status": status,
            "shape": self.shape,
            "full_chunk_elements": self.chunk_budget,
            "chunks": self.chunks,
            "max_chunk_elements_observed": self.max_chunk_elements_observed,
            "total_elements": self.total_elements,
            "elements_visited": self.elements_visited,
            "coverage_complete": self.elements_visited == self.total_elements,
            "comparison_domain": "left_and_right_pixel_masks_and_finite_values",
            "count": self.paired_count,
            "comparison_domain_count": self.paired_count,
            "topology": {
                "mask_equal": self.mask_mismatch == 0,
                "mask_mismatch_count": self.mask_mismatch,
                "left_masked_count": self.left_masked,
                "right_masked_count": self.right_masked,
                "finite_equal": self.finite_topology_mismatch == 0,
                "finite_topology_mismatch_count": self.finite_topology_mismatch,
                "nonfinite_kind_equal": self.nonfinite_kind_mismatch == 0,
                "nonfinite_kind_mismatch_count": self.nonfinite_kind_mismatch,
                "left_finite_count": self.left_finite,
                "right_finite_count": self.right_finite,
                "left_nonfinite": {
                    "nan": self.left_nan,
                    "positive_infinity": self.left_posinf,
                    "negative_infinity": self.left_neginf,
                },
                "right_nonfinite": {
                    "nan": self.right_nan,
                    "positive_infinity": self.right_posinf,
                    "negative_infinity": self.right_neginf,
                },
            },
            "left": {
                "min": finite_float(self.left_min) if self.paired_count else None,
                "max": finite_float(self.left_max) if self.paired_count else None,
                "sum": finite_float(self.left_sum.value()),
                "sum_squares": finite_float(self.left_sum_squares.value()),
                "rms": finite_float(left_rms),
                "integrated_value": finite_float(self.left_sum.value()),
                "peak_abs": self.left_peak,
            },
            "right": {
                "min": finite_float(self.right_min) if self.paired_count else None,
                "max": finite_float(self.right_max) if self.paired_count else None,
                "sum": finite_float(self.right_sum.value()),
                "sum_squares": finite_float(self.right_sum_squares.value()),
                "rms": finite_float(right_rms),
                "integrated_value": finite_float(self.right_sum.value()),
                "peak_abs": self.right_peak,
            },
            "cross_sum": finite_float(self.cross_sum.value()),
            "covariance": finite_float(covariance),
            "correlation": finite_float(correlation),
            "left_integrated_value": finite_float(self.left_sum.value()),
            "right_integrated_value": finite_float(self.right_sum.value()),
            "diff_integrated_value": finite_float(self.diff_sum.value()),
            "left_peak_abs": self.left_peak,
            "right_peak_abs": self.right_peak,
            "diff_peak_abs": self.diff_peak,
            "difference": {
                "sum": finite_float(self.diff_sum.value()),
                "sum_squares": finite_float(self.diff_sum_squares.value()),
                "integrated_value": finite_float(self.diff_sum.value()),
                "rms": finite_float(diff_rms),
                "abs_max": finite_float(self.diff_abs_max),
                "peak_abs": self.diff_peak,
            },
            "diff_rms": finite_float(diff_rms),
            "diff_abs_max": finite_float(self.diff_abs_max),
            "diff_rms_over_right_rms": relative_difference_ratio(diff_rms, right_rms),
            "diff_abs_max_over_right_peak": relative_difference_ratio(
                self.diff_abs_max, right_peak
            ),
        }
        if self.spatial_structure is not None:
            structure = self.spatial_structure.metrics(
                self.structure_suffix, self.structure_beam_info
            )
            native_evidence = structure.get("native_spatial_evidence")
            if not isinstance(native_evidence, dict) or not native_evidence.get(
                "coverage_complete"
            ):
                result["status"] = "structure_coverage_incomplete"
            elif self.spatial_structure.proves_exact_zero_operands():
                structure = exact_zero_structure_evidence(
                    self.structure_suffix,
                    evidence_scope=("full_native_central_spatial_plane_disk_backed"),
                )
                structure["native_spatial_evidence"] = native_evidence
                structure["beam_info"] = native_structure_beam_info(
                    self.structure_beam_info
                )
            result["structured_difference"] = structure
        return result


def relative_difference_ratio(difference, reference):
    if difference is None or not math.isfinite(float(difference)):
        return None
    if reference is not None and math.isfinite(float(reference)) and reference != 0.0:
        return finite_float(float(difference) / abs(float(reference)))
    return 0.0 if float(difference) == 0.0 else None


def update_chunk_peak(current, data, valid, blc):
    if not np.any(valid):
        return current
    amplitude = np.where(valid, np.abs(data), -np.inf)
    local = np.unravel_index(int(np.argmax(amplitude)), amplitude.shape)
    candidate = {
        "location": [int(start + offset) for start, offset in zip(blc, local)],
        "value": finite_float(data[local]),
        "abs_value": finite_float(abs(data[local])),
    }
    if current is None or candidate["abs_value"] > current["abs_value"]:
        return candidate
    return current


def full_array_statistics(
    left_path,
    right_path,
    max_elements,
    image_factory=None,
    *,
    structure_suffix=None,
    structure_beam_info=None,
    structure_scratch_root=None,
    defer_structure_cleanup=False,
):
    left_tool = new_image_tool(image_factory)
    right_tool = new_image_tool(image_factory)
    reducer = None
    result = None
    failure = None
    try:
        left_tool.open(left_path)
        right_tool.open(right_path)
        left_shape = [int(value) for value in left_tool.shape()]
        right_shape = [int(value) for value in right_tool.shape()]
        if left_shape != right_shape:
            return {
                "status": "shape_mismatch",
                "left_shape": left_shape,
                "right_shape": right_shape,
            }
        reducer = FullArrayReducer(
            left_shape,
            max_elements,
            structure_suffix=structure_suffix,
            structure_beam_info=structure_beam_info,
            structure_scratch_root=structure_scratch_root,
        )
        increment = [1] * len(left_shape)
        for blc, trc, elements in iter_full_chunks(left_shape, max_elements):
            left_data = left_tool.getchunk(
                blc=blc,
                trc=trc,
                inc=increment,
                dropdeg=False,
                getmask=False,
            )
            right_data = right_tool.getchunk(
                blc=blc,
                trc=trc,
                inc=increment,
                dropdeg=False,
                getmask=False,
            )
            left_mask = left_tool.getchunk(
                blc=blc,
                trc=trc,
                inc=increment,
                dropdeg=False,
                getmask=True,
            )
            right_mask = right_tool.getchunk(
                blc=blc,
                trc=trc,
                inc=increment,
                dropdeg=False,
                getmask=True,
            )
            if np.asarray(left_data).size != elements:
                raise ValueError(
                    "left image returned an unexpected full-array chunk shape"
                )
            if np.asarray(right_data).size != elements:
                raise ValueError(
                    "right image returned an unexpected full-array chunk shape"
                )
            reducer.add(left_data, right_data, left_mask, right_mask, blc)
        result = reducer.result()
        return result
    except Exception as error:
        failure = f"{type(error).__name__}: {error}"
        raise
    finally:
        if reducer is not None:
            result_status = (result or {}).get("status")
            retain = bool(
                result is None
                or result_status != "compared"
                or (defer_structure_cleanup and structure_scratch_root is not None)
            )
            reducer.close(
                retain=retain,
                failure=failure
                or (result_status if result_status not in {None, "compared"} else None),
            )
        left_tool.close()
        right_tool.close()


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
    x_width = contiguous_threshold_width(
        np.abs(plane[:, peak_index[1]]), peak_index[0], half
    )
    y_width = contiguous_threshold_width(
        np.abs(plane[peak_index[0], :]), peak_index[1], half
    )
    beam_area = math.pi * x_width * y_width / (4.0 * math.log(2.0))
    block_side = max(1, int(round(math.sqrt(max(1.0, beam_area)))))
    return {
        "status": "estimated_from_psf",
        "estimation_method": "bounded_sampled_psf_plane",
        "coordinate_domain": "sampled_direction_pixels",
        "psf_path": psf_path,
        "sample_stride": psf["sample_stride"],
        "peak_location": [int(value) for value in peak_index],
        "peak_abs": finite_float(peak_abs),
        "fwhm_pixels": [int(x_width), int(y_width)],
        "beam_area_pixels": finite_float(beam_area),
        "beam_block_side_pixels": int(block_side),
    }


def estimate_native_beam_info(psf_path, max_elements, image_factory=None):
    """Measure the central PSF plane in native pixels without sampling it."""

    if image_factory is None and not os.path.isdir(psf_path):
        return {"status": "missing_psf", "psf_path": psf_path}
    tool = new_image_tool(image_factory)
    try:
        tool.open(psf_path)
        shape = [int(value) for value in tool.shape()]
        if len(shape) < 2:
            return {
                "status": "insufficient_dimensions",
                "psf_path": psf_path,
                "shape": shape,
            }
        extra_coordinates = [int(size // 2) for size in shape[2:]]
        peak_abs = -math.inf
        peak_location = None
        pixels_visited = 0
        for spatial_blc, spatial_trc, elements in iter_full_chunks(
            shape[:2], max_elements
        ):
            blc = [*spatial_blc, *extra_coordinates]
            trc = [*spatial_trc, *extra_coordinates]
            data = np.asarray(
                tool.getchunk(
                    blc=blc,
                    trc=trc,
                    inc=[1] * len(shape),
                    dropdeg=False,
                    getmask=False,
                ),
                dtype=np.float64,
            ).reshape(spatial_trc[0] - spatial_blc[0] + 1, -1)
            if data.size != elements:
                raise ValueError("native PSF chunk has an unexpected shape")
            pixels_visited += int(data.size)
            finite = np.isfinite(data)
            if not np.any(finite):
                continue
            amplitude = np.where(finite, np.abs(data), -np.inf)
            local = np.unravel_index(int(np.argmax(amplitude)), amplitude.shape)
            candidate = float(amplitude[local])
            if candidate > peak_abs:
                peak_abs = candidate
                peak_location = [
                    int(spatial_blc[0] + local[0]),
                    int(spatial_blc[1] + local[1]),
                ]
        expected_pixels = shape[0] * shape[1]
        if pixels_visited != expected_pixels:
            return {
                "status": "coverage_incomplete",
                "psf_path": psf_path,
                "pixels_visited": pixels_visited,
                "expected_pixels": expected_pixels,
            }
        if peak_location is None:
            return {"status": "no_finite_psf", "psf_path": psf_path}
        if not np.isfinite(peak_abs) or peak_abs <= 0.0:
            return {"status": "zero_psf_peak", "psf_path": psf_path}
        x_cross_section = read_native_cross_section(
            tool,
            shape,
            varying_axis=0,
            fixed_spatial_coordinate=peak_location[1],
            max_elements=max_elements,
        )
        y_cross_section = read_native_cross_section(
            tool,
            shape,
            varying_axis=1,
            fixed_spatial_coordinate=peak_location[0],
            max_elements=max_elements,
        )
    finally:
        tool.close()
    half = 0.5 * peak_abs
    x_width = contiguous_threshold_width(
        np.abs(x_cross_section), peak_location[0], half
    )
    y_width = contiguous_threshold_width(
        np.abs(y_cross_section), peak_location[1], half
    )
    beam_area = math.pi * x_width * y_width / (4.0 * math.log(2.0))
    block_side = max(1, int(round(math.sqrt(max(1.0, beam_area)))))
    return {
        "status": "estimated_from_psf",
        "estimation_method": (
            "streamed_native_central_plane_peak_and_native_cross_sections"
        ),
        "coordinate_domain": "native_direction_pixels",
        "psf_path": psf_path,
        "sample_stride": [1] * len(shape),
        "peak_location": peak_location,
        "peak_abs": finite_float(peak_abs),
        "fwhm_pixels": [int(x_width), int(y_width)],
        "beam_area_pixels": finite_float(beam_area),
        "beam_block_side_pixels": int(block_side),
        "native_plane_coverage": {
            "pixels_visited": pixels_visited,
            "expected_pixels": expected_pixels,
            "coverage_complete": True,
        },
    }


def read_native_cross_section(
    tool,
    shape,
    *,
    varying_axis,
    fixed_spatial_coordinate,
    max_elements,
):
    values = np.empty(shape[varying_axis], dtype=np.float64)
    extra_coordinates = [int(size // 2) for size in shape[2:]]
    for start in range(0, shape[varying_axis], max_elements):
        end = min(shape[varying_axis], start + max_elements)
        spatial_blc = [fixed_spatial_coordinate, fixed_spatial_coordinate]
        spatial_trc = [fixed_spatial_coordinate, fixed_spatial_coordinate]
        spatial_blc[varying_axis] = start
        spatial_trc[varying_axis] = end - 1
        data = np.asarray(
            tool.getchunk(
                blc=[*spatial_blc, *extra_coordinates],
                trc=[*spatial_trc, *extra_coordinates],
                inc=[1] * len(shape),
                dropdeg=False,
                getmask=False,
            ),
            dtype=np.float64,
        ).reshape(-1)
        if data.size != end - start:
            raise ValueError("native PSF cross-section chunk has an unexpected shape")
        values[start:end] = data
    return values


def native_structure_beam_info(beam_info):
    adjusted = dict(beam_info) if isinstance(beam_info, dict) else {}
    if (
        adjusted.get("status") == "estimated_from_psf"
        and adjusted.get("coordinate_domain") != "native_direction_pixels"
    ):
        return {
            "status": "invalid_beam_coordinate_domain",
            "reason": "full native structure metrics require native PSF pixels",
            "provided": adjusted,
        }
    return adjusted


def contiguous_threshold_width(values, center, threshold):
    center = int(center)
    lower = center
    upper = center
    while lower > 0 and values[lower - 1] >= threshold:
        lower -= 1
    while upper + 1 < values.size and values[upper + 1] >= threshold:
        upper += 1
    return int(upper - lower + 1)


def structured_difference_metrics(
    suffix,
    rust_data,
    casa_data,
    diff_data,
    beam_info,
    *,
    scratch_root=None,
):
    rust_plane = display_plane(rust_data)
    casa_plane = display_plane(casa_data)
    diff_plane = display_plane(diff_data)
    if rust_plane.shape != casa_plane.shape or rust_plane.shape != diff_plane.shape:
        raise ValueError("structured-difference planes must have matching shapes")
    beam_side = 1
    if isinstance(beam_info, dict) and beam_info.get("status") == "estimated_from_psf":
        beam_side = max(1, int(beam_info.get("beam_block_side_pixels") or 1))
    mask_plan = streamed_structure_mask_plan(
        suffix,
        rust_plane,
        casa_plane,
        diff_plane,
        beam_side,
    )
    mask_description = mask_plan["description"]
    masked_pixels = mask_plan["masked_pixels"]
    if masked_pixels == 0:
        return {
            "status": "no_masked_pixels",
            "mask": mask_description,
            "finite_overlap": mask_plan["finite_overlap"],
            "beam_info_status": beam_info.get("status")
            if isinstance(beam_info, dict)
            else None,
        }

    basic = streamed_structure_basic_statistics(
        rust_plane,
        casa_plane,
        diff_plane,
        mask_plan=mask_plan,
    )
    flux_norm = basic["casa_rms"]
    if not flux_norm or not np.isfinite(flux_norm):
        flux_norm = basic["casa_abs_max"]
    diff_rms = basic["diff_rms"]
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
            "masked_pixels": masked_pixels,
            "analysis_pixels": mask_plan["analysis_pixels"],
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
    scratch_parent = (
        str(pathlib.Path(scratch_root)) if scratch_root is not None else None
    )
    with tempfile.TemporaryDirectory(
        prefix=".bounded-structure-analysis-", dir=scratch_parent
    ) as scratch_directory:
        scratch_path = pathlib.Path(scratch_directory)
        low_order_r2 = low_order_r2_score(
            diff_plane,
            None,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            mask_plan=mask_plan,
        )
        large_scale_power = large_scale_power_fraction(
            diff_plane,
            None,
            beam_side,
            min_wavelength_beams=8.0,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            scratch_root=scratch_path,
            mask_plan=mask_plan,
        )
        basis_fit = difference_basis_fit(
            casa_plane,
            diff_plane,
            None,
            rust_plane=rust_plane,
            mask_plan=mask_plan,
        )
        block_metrics = beam_block_metrics(
            diff_plane,
            None,
            beam_side,
            flux_norm,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            scratch_root=scratch_path,
            mask_plan=mask_plan,
        )
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
        "masked_pixels": masked_pixels,
        "analysis_pixels": mask_plan["analysis_pixels"],
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


def exact_zero_structure_evidence(suffix, *, evidence_scope):
    """Represent the one zero-reference case that is provably not applicable."""

    label = "not_applicable_exact_zero"
    classification = {
        "overall": label,
        "amplitude": label,
        "structure": label,
        "structure_components": {},
        "thresholds": structured_difference_thresholds(),
    }
    return {
        "status": label,
        "evidence_scope": evidence_scope,
        "normalization": {
            "type": "exact_zero_operands",
            "value": 0.0,
        },
        "diff_rms": 0.0,
        "normalized_diff_rms": None,
        "low_order_r2_quadratic": None,
        "large_scale_power_fraction": None,
        "scale_offset_gradient_fit": {
            "status": label,
            "reason": "both operands and their difference are exactly zero",
        },
        "beam_block_rms_by_scale": [],
        "block_rms_decay_slope_vs_independent_beams": None,
        "classification": classification,
        "review": {
            "label": label,
            "summary": (
                f"{suffix}: structured difference is not applicable because full "
                "evidence proves both operands and their difference are exactly zero."
            ),
            "checks": [
                {
                    "name": "exact_zero_operands",
                    "label": label,
                    "value": True,
                    "meaning": "full comparison domain proves both operands are zero",
                }
            ],
            "legend": structured_difference_review_legend(),
        },
    }


def incomplete_structure_coverage_evidence(suffix, native_evidence):
    label = "unknown"
    return {
        "status": "structure_coverage_incomplete",
        "evidence_scope": "full_native_central_spatial_plane_disk_backed",
        "native_spatial_evidence": native_evidence,
        "normalization": None,
        "diff_rms": None,
        "normalized_diff_rms": None,
        "low_order_r2_quadratic": None,
        "large_scale_power_fraction": None,
        "scale_offset_gradient_fit": {
            "status": "not_run",
            "reason": "native central-plane coverage is incomplete or overlapping",
        },
        "beam_block_rms_by_scale": [],
        "block_rms_decay_slope_vs_independent_beams": None,
        "classification": {
            "overall": label,
            "amplitude": label,
            "structure": label,
            "structure_components": {},
            "thresholds": structured_difference_thresholds(),
        },
        "review": {
            "label": label,
            "summary": (
                f"{suffix}: unknown; exact native central-plane coverage is "
                "incomplete or overlapping."
            ),
            "checks": [
                {
                    "name": "native_spatial_coverage",
                    "label": label,
                    "value": False,
                    "meaning": "every native central-plane pixel must be written once",
                }
            ],
            "legend": structured_difference_review_legend(),
        },
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
                base_mask
                & ((np.abs(rust_plane) > threshold) | (np.abs(casa_plane) > threshold)),
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
    if suffix == ".weight":
        return base_mask, {"type": "finite_overlap"}
    if product_family(suffix) in {".pb", ".weight"}:
        return base_mask, {
            "type": "full_finite_overlap",
            "product_family": product_family(suffix),
        }
    return base_mask, {"type": "finite_overlap"}


def non_spatial_product(suffix):
    return product_family(suffix) == ".sumwt"


def product_family(suffix):
    for family in (".image", ".residual", ".model", ".psf", ".sumwt", ".pb", ".weight"):
        if suffix == family or suffix.startswith(family + ".tt"):
            return family
    return suffix


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


def _structure_row_ranges(shape, *, bytes_per_pixel):
    height, width = (int(value) for value in shape)
    row_bytes = max(1, width * int(bytes_per_pixel))
    rows = max(1, min(height, STRUCTURE_WORKING_BYTES // row_bytes))
    for start in range(0, height, rows):
        yield start, min(height, start + rows)


def _finite_overlap_chunk(rust_plane, casa_plane, diff_plane, start, end):
    rust = np.asarray(rust_plane[start:end], dtype=np.float64)
    casa = np.asarray(casa_plane[start:end], dtype=np.float64)
    diff = np.asarray(diff_plane[start:end], dtype=np.float64)
    return np.isfinite(rust) & np.isfinite(casa) & np.isfinite(diff)


def _streamed_finite_overlap_and_weight_scale(
    rust_plane,
    casa_plane,
    diff_plane,
):
    finite_count = 0
    rust_abs_max = 0.0
    casa_abs_max = 0.0
    for start, end in _structure_row_ranges(diff_plane.shape, bytes_per_pixel=48):
        valid = _finite_overlap_chunk(
            rust_plane,
            casa_plane,
            diff_plane,
            start,
            end,
        )
        count = int(np.count_nonzero(valid))
        if count == 0:
            continue
        finite_count += count
        rust_values = np.asarray(rust_plane[start:end], dtype=np.float64)[valid]
        casa_values = np.asarray(casa_plane[start:end], dtype=np.float64)[valid]
        rust_abs_max = max(rust_abs_max, float(np.max(np.abs(rust_values))))
        casa_abs_max = max(casa_abs_max, float(np.max(np.abs(casa_values))))
    return finite_count, max(rust_abs_max, casa_abs_max)


def _structured_product_mask_chunk(
    plan,
    rust_plane,
    casa_plane,
    diff_plane,
    start,
    end,
    *,
    analysis,
):
    valid = _finite_overlap_chunk(
        rust_plane,
        casa_plane,
        diff_plane,
        start,
        end,
    )
    if plan["mask_type"] == "weight_union_support":
        threshold = plan["weight_threshold"]
        rust = np.asarray(rust_plane[start:end], dtype=np.float64)
        casa = np.asarray(casa_plane[start:end], dtype=np.float64)
        valid &= (np.abs(rust) > threshold) | (np.abs(casa) > threshold)
    elif plan["mask_type"] == "casa_pb_support":
        casa = np.asarray(casa_plane[start:end], dtype=np.float64)
        valid &= casa > 0.01

    if not analysis or not plan["use_erosion"]:
        return valid
    radius = plan["erosion_radius"]
    height, width = diff_plane.shape
    valid[:, :radius] = False
    valid[:, width - radius :] = False
    if start < radius:
        valid[: min(end, radius) - start] = False
    lower_border = height - radius
    if end > lower_border:
        valid[max(0, lower_border - start) :] = False
    return valid


def streamed_structure_mask_plan(
    suffix,
    rust_plane,
    casa_plane,
    diff_plane,
    beam_side,
):
    finite_overlap, weight_scale = _streamed_finite_overlap_and_weight_scale(
        rust_plane,
        casa_plane,
        diff_plane,
    )
    if suffix == ".weight" and weight_scale > 0.0:
        threshold = 1.0e-3 * weight_scale
        mask_type = "weight_union_support"
        description = {
            "type": mask_type,
            "threshold_fraction_of_peak": 1.0e-3,
            "threshold": finite_float(threshold),
        }
    elif suffix == ".pb":
        threshold = None
        mask_type = "casa_pb_support"
        description = {"type": mask_type, "threshold": 0.01}
    elif suffix == ".weight":
        threshold = None
        mask_type = "finite_overlap"
        description = {"type": mask_type}
    elif product_family(suffix) in {".pb", ".weight"}:
        threshold = None
        mask_type = "finite_overlap"
        description = {
            "type": "full_finite_overlap",
            "product_family": product_family(suffix),
        }
    else:
        threshold = None
        mask_type = "finite_overlap"
        description = {"type": mask_type}

    radius = int(max(1, round(beam_side)))
    height, width = diff_plane.shape
    can_erode = (
        suffix in {".pb", ".weight"}
        and beam_side > 1
        and height > 2 * radius
        and width > 2 * radius
    )
    plan = {
        "mask_type": mask_type,
        "weight_threshold": threshold,
        "description": description,
        "finite_overlap": finite_overlap,
        "erosion_radius": radius,
        "use_erosion": False,
    }
    masked_pixels = 0
    eroded_pixels = 0
    for start, end in _structure_row_ranges(diff_plane.shape, bytes_per_pixel=80):
        masked_pixels += int(
            np.count_nonzero(
                _structured_product_mask_chunk(
                    plan,
                    rust_plane,
                    casa_plane,
                    diff_plane,
                    start,
                    end,
                    analysis=False,
                )
            )
        )
        if can_erode:
            plan["use_erosion"] = True
            eroded_pixels += int(
                np.count_nonzero(
                    _structured_product_mask_chunk(
                        plan,
                        rust_plane,
                        casa_plane,
                        diff_plane,
                        start,
                        end,
                        analysis=True,
                    )
                )
            )
            plan["use_erosion"] = False
    plan["masked_pixels"] = masked_pixels
    plan["use_erosion"] = can_erode and eroded_pixels > 0
    plan["analysis_pixels"] = eroded_pixels if plan["use_erosion"] else masked_pixels
    return plan


def streamed_structure_basic_statistics(
    rust_plane,
    casa_plane,
    diff_plane,
    *,
    mask_plan=None,
):
    finite_count = 0
    casa_sum_squares = CompensatedSum()
    diff_sum_squares = CompensatedSum()
    casa_abs_max = 0.0
    for start, end in _structure_row_ranges(diff_plane.shape, bytes_per_pixel=64):
        valid = _metric_mask_chunk(
            diff_plane,
            None,
            start,
            end,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            mask_plan=mask_plan,
        )
        count = int(np.count_nonzero(valid))
        if count == 0:
            continue
        finite_count += count
        casa_values = np.asarray(casa_plane[start:end], dtype=np.float64)[valid]
        diff_values = np.asarray(diff_plane[start:end], dtype=np.float64)[valid]
        with np.errstate(over="ignore", invalid="ignore"):
            casa_sum_squares.add(float(np.sum(casa_values * casa_values)))
            diff_sum_squares.add(float(np.sum(diff_values * diff_values)))
        casa_abs_max = max(casa_abs_max, float(np.max(np.abs(casa_values))))
    if finite_count == 0:
        return {
            "finite_count": 0,
            "casa_rms": 0.0,
            "casa_abs_max": 0.0,
            "diff_rms": None,
        }
    with np.errstate(over="ignore", invalid="ignore"):
        casa_rms = math.sqrt(casa_sum_squares.value() / finite_count)
        diff_rms = math.sqrt(diff_sum_squares.value() / finite_count)
    return {
        "finite_count": finite_count,
        "casa_rms": float(casa_rms),
        "casa_abs_max": float(casa_abs_max),
        "diff_rms": float(diff_rms),
    }


def _metric_mask_chunk(
    data,
    mask,
    start,
    end,
    *,
    rust_plane=None,
    casa_plane=None,
    mask_plan=None,
):
    if mask_plan is not None:
        if rust_plane is None or casa_plane is None:
            raise ValueError("streamed structure mask requires both operands")
        return _structured_product_mask_chunk(
            mask_plan,
            rust_plane,
            casa_plane,
            data,
            start,
            end,
            analysis=True,
        )
    if mask is not None:
        return np.asarray(mask[start:end], dtype=bool)
    valid = np.isfinite(np.asarray(data[start:end], dtype=np.float64))
    if rust_plane is not None:
        valid &= np.isfinite(np.asarray(rust_plane[start:end], dtype=np.float64))
    if casa_plane is not None:
        valid &= np.isfinite(np.asarray(casa_plane[start:end], dtype=np.float64))
    return valid


class StreamingLeastSquares:
    """Bounded QR sufficient summary for an otherwise row-large fit."""

    def __init__(self, columns):
        self.columns = int(columns)
        self.rows = 0
        self.reduced_design = np.empty((0, self.columns), dtype=np.float64)
        self.reduced_target = np.empty(0, dtype=np.float64)
        self.target_sum_squares = CompensatedSum()

    def add(self, design, target):
        design = np.asarray(design, dtype=np.float64)
        target = np.asarray(target, dtype=np.float64)
        if design.ndim != 2 or design.shape[1] != self.columns:
            raise ValueError("streaming least-squares design has the wrong shape")
        if target.ndim != 1 or target.size != design.shape[0]:
            raise ValueError("streaming least-squares target has the wrong shape")
        if target.size == 0:
            return
        self.rows += int(target.size)
        self.target_sum_squares.add(float(np.sum(target * target)))
        combined_design = np.vstack((self.reduced_design, design))
        combined_target = np.concatenate((self.reduced_target, target))
        q_matrix, reduced_design = np.linalg.qr(combined_design, mode="reduced")
        self.reduced_design = reduced_design
        self.reduced_target = q_matrix.T @ combined_target

    def solve(self):
        if self.rows == 0:
            return None, None
        # np.linalg.lstsq(rcond=None) uses eps * max(M, N).  The QR summary has
        # only O(columns) rows, so pass the original row-count cutoff explicitly
        # to preserve the rank decision of the former full design matrix.
        rcond = np.finfo(np.float64).eps * max(self.rows, self.columns)
        coefficients, *_ = np.linalg.lstsq(
            self.reduced_design,
            self.reduced_target,
            rcond=rcond,
        )
        reduced_residual = self.reduced_design @ coefficients - self.reduced_target
        residual_sum_squares = (
            self.target_sum_squares.value()
            - float(np.sum(self.reduced_target * self.reduced_target))
            + float(np.sum(reduced_residual * reduced_residual))
        )
        scale = max(1.0, abs(self.target_sum_squares.value()))
        if residual_sum_squares < 0.0 and abs(residual_sum_squares) <= 1.0e-12 * scale:
            residual_sum_squares = 0.0
        return coefficients, float(residual_sum_squares)


class StreamingMoments:
    """Numerically stable count/mean/variance summary for bounded chunks."""

    def __init__(self):
        self.count = 0
        self.mean = 0.0
        self.sum_squared_deviations = 0.0

    def add(self, values):
        values = np.asarray(values, dtype=np.float64)
        if values.size == 0:
            return
        batch_count = int(values.size)
        batch_mean = float(np.mean(values))
        centered = values - batch_mean
        batch_deviations = float(np.sum(centered * centered))
        if self.count == 0:
            self.count = batch_count
            self.mean = batch_mean
            self.sum_squared_deviations = batch_deviations
            return
        combined_count = self.count + batch_count
        delta = batch_mean - self.mean
        self.sum_squared_deviations += batch_deviations + delta * delta * float(
            self.count
        ) * float(batch_count) / float(combined_count)
        self.mean += delta * float(batch_count) / float(combined_count)
        self.count = combined_count


def _quadratic_basis(x, y):
    basis = np.empty((x.size, 7), dtype=np.float64)
    basis[:, 0] = 1.0
    basis[:, 1] = x
    basis[:, 2] = y
    basis[:, 3] = x * x
    basis[:, 4] = x * y
    basis[:, 5] = y * y
    basis[:, 6] = basis[:, 3] + basis[:, 5]
    return basis


def low_order_r2_score(
    data,
    mask,
    *,
    rust_plane=None,
    casa_plane=None,
    mask_plan=None,
):
    count = 0
    x_sum = 0
    y_sum = 0
    x_min = data.shape[1]
    x_max = -1
    y_min = data.shape[0]
    y_max = -1
    z_sum = CompensatedSum()
    for start, end in _structure_row_ranges(data.shape, bytes_per_pixel=64):
        valid = _metric_mask_chunk(
            data,
            mask,
            start,
            end,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            mask_plan=mask_plan,
        )
        local_y, local_x = np.nonzero(valid)
        if local_x.size == 0:
            continue
        global_y = local_y + start
        count += int(local_x.size)
        x_sum += int(np.sum(local_x, dtype=np.int64))
        y_sum += int(np.sum(global_y, dtype=np.int64))
        x_min = min(x_min, int(np.min(local_x)))
        x_max = max(x_max, int(np.max(local_x)))
        y_min = min(y_min, int(np.min(global_y)))
        y_max = max(y_max, int(np.max(global_y)))
        z_sum.add(float(np.sum(np.asarray(data[start:end], dtype=np.float64)[valid])))
    if count < 8:
        return None
    x_mean = float(x_sum) / count
    y_mean = float(y_sum) / count
    z_mean = z_sum.value() / count
    x_scale = 0.5 * float(x_max - x_min) if x_max != x_min else 1.0
    y_scale = 0.5 * float(y_max - y_min) if y_max != y_min else 1.0
    fit = StreamingLeastSquares(7)
    for start, end in _structure_row_ranges(data.shape, bytes_per_pixel=256):
        valid = _metric_mask_chunk(
            data,
            mask,
            start,
            end,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            mask_plan=mask_plan,
        )
        local_y, local_x = np.nonzero(valid)
        if local_x.size == 0:
            continue
        x = (local_x.astype(np.float64) - x_mean) / x_scale
        y = (local_y.astype(np.float64) + start - y_mean) / y_scale
        z = np.asarray(data[start:end], dtype=np.float64)[valid] - z_mean
        fit.add(_quadratic_basis(x, y), z)
    total = fit.target_sum_squares.value()
    if total <= 0.0 or not np.isfinite(total):
        return None
    coefficients, _ = fit.solve()
    residual_sum_squares = CompensatedSum()
    for start, end in _structure_row_ranges(data.shape, bytes_per_pixel=256):
        valid = _metric_mask_chunk(
            data,
            mask,
            start,
            end,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            mask_plan=mask_plan,
        )
        local_y, local_x = np.nonzero(valid)
        if local_x.size == 0:
            continue
        x = (local_x.astype(np.float64) - x_mean) / x_scale
        y = (local_y.astype(np.float64) + start - y_mean) / y_scale
        z = np.asarray(data[start:end], dtype=np.float64)[valid] - z_mean
        residual = z - _quadratic_basis(x, y) @ coefficients
        residual_sum_squares.add(float(np.sum(residual * residual)))
    return 1.0 - residual_sum_squares.value() / total


def _masked_pixel_count(
    data,
    mask,
    *,
    rust_plane=None,
    casa_plane=None,
    mask_plan=None,
):
    count = 0
    for start, end in _structure_row_ranges(data.shape, bytes_per_pixel=32):
        count += int(
            np.count_nonzero(
                _metric_mask_chunk(
                    data,
                    mask,
                    start,
                    end,
                    rust_plane=rust_plane,
                    casa_plane=casa_plane,
                    mask_plan=mask_plan,
                )
            )
        )
    return count


def _gradient_fit_chunks(
    reference,
    diff,
    mask,
    *,
    rust_plane=None,
    mask_plan=None,
):
    height, width = reference.shape
    for start, end in _structure_row_ranges(reference.shape, bytes_per_pixel=128):
        halo_start = max(0, start - 1)
        halo_end = min(height, end + 1)
        reference_halo = np.asarray(reference[halo_start:halo_end], dtype=np.float64)
        center_start = start - halo_start
        center_end = center_start + (end - start)
        reference_values = reference_halo[center_start:center_end]
        diff_values = np.asarray(diff[start:end], dtype=np.float64)
        x_gradient = np.empty_like(reference_values)
        y_gradient = np.empty_like(reference_values)
        x_gradient[:, 0] = reference_values[:, 1] - reference_values[:, 0]
        x_gradient[:, -1] = reference_values[:, -1] - reference_values[:, -2]
        if width > 2:
            x_gradient[:, 1:-1] = (
                reference_values[:, 2:] - reference_values[:, :-2]
            ) / 2.0
        if start == 0:
            y_gradient[0] = reference_halo[1] - reference_halo[0]
        if end == height:
            y_gradient[-1] = reference_halo[-1] - reference_halo[-2]
        interior_start = max(start, 1)
        interior_end = min(end, height - 1)
        if interior_start < interior_end:
            output_start = interior_start - start
            output_end = interior_end - start
            upper_start = interior_start + 1 - halo_start
            upper_end = interior_end + 1 - halo_start
            lower_start = interior_start - 1 - halo_start
            lower_end = interior_end - 1 - halo_start
            y_gradient[output_start:output_end] = (
                reference_halo[upper_start:upper_end]
                - reference_halo[lower_start:lower_end]
            ) / 2.0
        valid = _metric_mask_chunk(
            diff,
            mask,
            start,
            end,
            rust_plane=rust_plane,
            casa_plane=reference,
            mask_plan=mask_plan,
        )
        valid &= (
            np.isfinite(reference_values)
            & np.isfinite(diff_values)
            & np.isfinite(x_gradient)
            & np.isfinite(y_gradient)
        )
        if not np.any(valid):
            continue
        yield (
            reference_values[valid],
            diff_values[valid],
            x_gradient[valid],
            y_gradient[valid],
        )


def difference_basis_fit(
    reference,
    diff,
    mask,
    *,
    rust_plane=None,
    mask_plan=None,
):
    masked_pixels = _masked_pixel_count(
        diff,
        mask,
        rust_plane=rust_plane,
        casa_plane=reference,
        mask_plan=mask_plan,
    )
    if masked_pixels < 8:
        return {"status": "insufficient_pixels"}
    if reference.ndim != 2 or min(reference.shape) < 2:
        return {
            "status": "insufficient_dimensions",
            "shape": [int(v) for v in reference.shape],
        }
    fit = StreamingLeastSquares(4)
    for reference_values, diff_values, x_gradient, y_gradient in _gradient_fit_chunks(
        reference,
        diff,
        mask,
        rust_plane=rust_plane,
        mask_plan=mask_plan,
    ):
        basis = np.empty((diff_values.size, 4), dtype=np.float64)
        basis[:, 0] = reference_values
        basis[:, 1] = 1.0
        basis[:, 2] = x_gradient
        basis[:, 3] = y_gradient
        fit.add(basis, diff_values)
    fit_pixels = fit.rows
    excluded_nonfinite_basis_pixels = masked_pixels - fit_pixels
    if fit_pixels < 8:
        return {
            "status": "insufficient_finite_basis_pixels",
            "masked_pixels": masked_pixels,
            "fit_pixels": fit_pixels,
            "excluded_nonfinite_basis_pixels": excluded_nonfinite_basis_pixels,
        }
    coefficients, _ = fit.solve()
    residual_sum_squares = CompensatedSum()
    for reference_values, diff_values, x_gradient, y_gradient in _gradient_fit_chunks(
        reference,
        diff,
        mask,
        rust_plane=rust_plane,
        mask_plan=mask_plan,
    ):
        basis = np.empty((diff_values.size, 4), dtype=np.float64)
        basis[:, 0] = reference_values
        basis[:, 1] = 1.0
        basis[:, 2] = x_gradient
        basis[:, 3] = y_gradient
        residual = diff_values - basis @ coefficients
        residual_sum_squares.add(float(np.sum(residual * residual)))
    residual_sum = residual_sum_squares.value()
    # The schema-v4 validator binds this diagnostic to the uncentered
    # difference energy reported by diff_rms. Keep the producer and validator
    # on that same closed derivation rather than mixing centered variance with
    # an uncentered RMS field.
    total = fit.target_sum_squares.value()
    r2 = 1.0 - residual_sum / total if total > 0.0 and np.isfinite(total) else None
    return {
        "status": "computed",
        "model": "diff ~= scale*reference + offset + dx*d_reference_dx + dy*d_reference_dy",
        "masked_pixels": masked_pixels,
        "fit_pixels": fit_pixels,
        "excluded_nonfinite_basis_pixels": excluded_nonfinite_basis_pixels,
        "r2": finite_float(r2),
        "diff_rms": finite_float(
            math.sqrt(fit.target_sum_squares.value() / fit_pixels)
        ),
        "residual_rms": finite_float(math.sqrt(max(0.0, residual_sum) / fit_pixels)),
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
        "block_rms_decay_slope_vs_independent_beams": classify_block_decay(
            block_decay_slope
        ),
        "large_scale_power_fraction": classify_large_scale_power(
            large_scale_power.get("fraction")
            if isinstance(large_scale_power, dict)
            else None
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
        large_scale_power.get("fraction")
        if isinstance(large_scale_power, dict)
        else None
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
        "not_applicable_exact_zero": (
            "Full evidence proves both operands and their difference are exactly zero."
        ),
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
    rank = {
        "not_applicable_exact_zero": 0,
        "good": 1,
        "investigate": 2,
        "unknown": 3,
        "bad": 4,
    }
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


def structure_analysis_storage_plan(shape):
    """Return the mechanically checked RAM/disk plan for one native plane."""

    if len(shape) != 2 or any(int(value) < 1 for value in shape):
        raise ValueError(
            "structure analysis requires a non-empty two-dimensional shape"
        )
    height, width = (int(value) for value in shape)
    pixels = height * width
    frequency_columns = width // 2 + 1
    return {
        "shape": [height, width],
        "native_pixels": pixels,
        "resident_working_budget_bytes": STRUCTURE_WORKING_BYTES,
        "fft_intermediate_complex128_bytes": height * frequency_columns * 16,
        "maximum_block_stat_float64_bytes": pixels * 2 * 8,
        "full_plane_ram_arrays": 0,
    }


def large_scale_power_fraction(
    data,
    mask,
    beam_side,
    min_wavelength_beams,
    *,
    rust_plane=None,
    casa_plane=None,
    scratch_root=None,
    mask_plan=None,
):
    if scratch_root is None:
        with tempfile.TemporaryDirectory(prefix=".bounded-radial-power-") as temporary:
            return large_scale_power_fraction(
                data,
                mask,
                beam_side,
                min_wavelength_beams,
                rust_plane=rust_plane,
                casa_plane=casa_plane,
                scratch_root=pathlib.Path(temporary),
                mask_plan=mask_plan,
            )
    count = 0
    value_sum = CompensatedSum()
    for start, end in _structure_row_ranges(data.shape, bytes_per_pixel=48):
        values = np.asarray(data[start:end], dtype=np.float64)
        valid = _metric_mask_chunk(
            data,
            mask,
            start,
            end,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            mask_plan=mask_plan,
        )
        valid &= np.isfinite(values)
        finite_values = values[valid]
        if finite_values.size:
            count += int(finite_values.size)
            value_sum.add(float(np.sum(finite_values)))
    if count < 4:
        return None
    mean = value_sum.value() / count
    height, width = data.shape
    frequency_columns = width // 2 + 1
    spectrum_path = pathlib.Path(scratch_root) / "radial-power-spectrum.c128"
    spectrum = np.memmap(
        spectrum_path,
        mode="w+",
        dtype=np.complex128,
        shape=(height, frequency_columns),
    )
    try:
        for start, end in _structure_row_ranges(data.shape, bytes_per_pixel=80):
            values = np.asarray(data[start:end], dtype=np.float64)
            valid = _metric_mask_chunk(
                data,
                mask,
                start,
                end,
                rust_plane=rust_plane,
                casa_plane=casa_plane,
                mask_plan=mask_plan,
            )
            valid &= np.isfinite(values)
            centered = np.zeros(values.shape, dtype=np.float64)
            np.subtract(values, mean, out=centered, where=valid)
            spectrum[start:end] = np.fft.rfft(centered, axis=1)
        spectrum.flush()

        cutoff = 1.0 / max(1.0, float(beam_side) * float(min_wavelength_beams))
        y_frequency_squared = np.fft.fftfreq(height) ** 2
        x_frequencies = np.fft.rfftfreq(width)
        bytes_per_column = max(1, height * 80)
        column_batch = max(
            1, min(frequency_columns, STRUCTURE_WORKING_BYTES // bytes_per_column)
        )
        total_power = CompensatedSum()
        selected_power = CompensatedSum()
        for start in range(0, frequency_columns, column_batch):
            end = min(frequency_columns, start + column_batch)
            transformed = np.fft.fft(spectrum[:, start:end], axis=0)
            power = np.square(np.abs(transformed))
            radii = np.sqrt(
                y_frequency_squared[:, np.newaxis]
                + np.square(x_frequencies[start:end])[np.newaxis, :]
            )
            selected = radii <= cutoff
            if start == 0:
                power[0, 0] = 0.0
                selected[0, 0] = False
            total_power.add(float(np.sum(power)))
            selected_power.add(float(np.sum(power[selected])))
        total = total_power.value()
        if total <= 0.0 or not np.isfinite(total):
            return None
        return {
            "min_wavelength_beams": finite_float(min_wavelength_beams),
            "frequency_cutoff_cycles_per_pixel": finite_float(cutoff),
            "fraction": finite_float(selected_power.value() / total),
        }
    finally:
        spectrum.flush()
        del spectrum
        if spectrum_path.exists():
            spectrum_path.unlink()


def beam_block_metrics(
    data,
    mask,
    beam_side,
    flux_norm,
    *,
    rust_plane=None,
    casa_plane=None,
    scratch_root=None,
    mask_plan=None,
):
    if scratch_root is None:
        with tempfile.TemporaryDirectory(
            prefix=".bounded-block-statistics-"
        ) as temporary:
            return beam_block_metrics(
                data,
                mask,
                beam_side,
                flux_norm,
                rust_plane=rust_plane,
                casa_plane=casa_plane,
                scratch_root=pathlib.Path(temporary),
                mask_plan=mask_plan,
            )
    scales = []
    block_sides = []
    normalized_rms_values = []
    independent_beam_counts = []
    for multiplier in [1, 2, 4, 8, 16, 32]:
        side = max(1, int(round(float(beam_side) * multiplier)))
        metric = block_metric_for_side(
            data,
            mask,
            side,
            beam_side,
            flux_norm,
            multiplier,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            scratch_root=pathlib.Path(scratch_root),
            mask_plan=mask_plan,
        )
        if metric is None:
            continue
        scales.append(metric)
        normalized = metric.get("normalized_block_mean_rms")
        independent_beams = metric.get("approx_independent_beams_per_block")
        if (
            normalized is not None
            and normalized > 0.0
            and independent_beams
            and independent_beams > 0.0
        ):
            block_sides.append(side)
            normalized_rms_values.append(float(normalized))
            independent_beam_counts.append(float(independent_beams))
    slope = None
    if len(normalized_rms_values) >= 2:
        x = np.log(np.asarray(independent_beam_counts, dtype=np.float64))
        y = np.log(np.asarray(normalized_rms_values, dtype=np.float64))
        if np.all(np.isfinite(x)) and np.all(np.isfinite(y)) and np.ptp(x) > 0.0:
            slope = float(np.polyfit(x, y, 1)[0])
    return {
        "scales": scales,
        "decay_slope": finite_float(slope) if slope is not None else None,
    }


def _block_slab_summaries(
    data,
    mask,
    side,
    *,
    rust_plane,
    casa_plane,
    mask_plan,
):
    height, width = data.shape
    x_blocks = int(math.ceil(float(width) / side))
    y_blocks = int(math.ceil(float(height) / side))
    padded_width = x_blocks * side
    one_block_row_bytes = max(1, side * padded_width * 80)
    if one_block_row_bytes <= STRUCTURE_WORKING_BYTES:
        block_rows_per_slab = max(1, STRUCTURE_WORKING_BYTES // one_block_row_bytes)
        for y_block_start in range(0, y_blocks, block_rows_per_slab):
            slab_block_rows = min(block_rows_per_slab, y_blocks - y_block_start)
            start = y_block_start * side
            end = min(height, (y_block_start + slab_block_rows) * side)
            padded_height = slab_block_rows * side
            values = np.zeros((padded_height, padded_width), dtype=np.float64)
            valid = np.zeros((padded_height, padded_width), dtype=bool)
            source_values = np.asarray(data[start:end], dtype=np.float64)
            source_valid = _metric_mask_chunk(
                data,
                mask,
                start,
                end,
                rust_plane=rust_plane,
                casa_plane=casa_plane,
                mask_plan=mask_plan,
            )
            rows = end - start
            valid[:rows, :width] = source_valid
            np.copyto(values[:rows, :width], source_values, where=source_valid)
            block_values = values.reshape(slab_block_rows, side, x_blocks, side)
            block_valid = valid.reshape(slab_block_rows, side, x_blocks, side)
            counts = np.sum(block_valid, axis=(1, 3), dtype=np.int64)
            sums = np.sum(block_values, axis=(1, 3), dtype=np.float64)
            sums_squares = np.sum(
                block_values * block_values, axis=(1, 3), dtype=np.float64
            )
            yield counts.reshape(-1), sums.reshape(-1), sums_squares.reshape(-1)
        return

    x_starts = np.arange(0, width, side, dtype=np.int64)
    maximum_rows = max(1, STRUCTURE_WORKING_BYTES // max(1, width * 64))
    for y_block_start in range(y_blocks):
        counts = np.zeros(x_blocks, dtype=np.int64)
        sums = np.zeros(x_blocks, dtype=np.float64)
        sums_squares = np.zeros(x_blocks, dtype=np.float64)
        block_start = y_block_start * side
        block_end = min(height, block_start + side)
        for start in range(block_start, block_end, maximum_rows):
            end = min(block_end, start + maximum_rows)
            source_values = np.asarray(data[start:end], dtype=np.float64)
            source_valid = _metric_mask_chunk(
                data,
                mask,
                start,
                end,
                rust_plane=rust_plane,
                casa_plane=casa_plane,
                mask_plan=mask_plan,
            )
            values = np.zeros(source_values.shape, dtype=np.float64)
            np.copyto(values, source_values, where=source_valid)
            counts += np.sum(
                np.add.reduceat(source_valid.astype(np.int64), x_starts, axis=1),
                axis=0,
                dtype=np.int64,
            )
            sums += np.sum(
                np.add.reduceat(values, x_starts, axis=1),
                axis=0,
                dtype=np.float64,
            )
            sums_squares += np.sum(
                np.add.reduceat(values * values, x_starts, axis=1),
                axis=0,
                dtype=np.float64,
            )
        yield counts, sums, sums_squares


def _memmap_median(values, count):
    view = values[:count]
    upper = count // 2
    lower = upper if count % 2 else upper - 1
    view.partition((lower, upper))
    if lower == upper:
        return float(view[upper])
    return float((view[lower] + view[upper]) / 2.0)


def _fill_memmap_transform(destination, source, count, transform):
    chunk = max(1, STRUCTURE_WORKING_BYTES // 32)
    maximum = 0.0
    for start in range(0, count, chunk):
        end = min(count, start + chunk)
        transformed = transform(np.asarray(source[start:end], dtype=np.float64))
        destination[start:end] = transformed
        if transformed.size:
            maximum = max(maximum, float(np.max(transformed)))
    destination.flush()
    return maximum


def block_metric_for_side(
    data,
    mask,
    side,
    beam_side,
    flux_norm,
    multiplier,
    *,
    rust_plane=None,
    casa_plane=None,
    scratch_root=None,
    mask_plan=None,
):
    if scratch_root is None:
        with tempfile.TemporaryDirectory(prefix=".bounded-block-side-") as temporary:
            return block_metric_for_side(
                data,
                mask,
                side,
                beam_side,
                flux_norm,
                multiplier,
                rust_plane=rust_plane,
                casa_plane=casa_plane,
                scratch_root=pathlib.Path(temporary),
                mask_plan=mask_plan,
            )
    height, width = data.shape
    maximum_blocks = int(math.ceil(float(height) / side)) * int(
        math.ceil(float(width) / side)
    )
    means_path = pathlib.Path(scratch_root) / f"block-means-{side}.f64"
    work_path = pathlib.Path(scratch_root) / f"block-work-{side}.f64"
    means = np.memmap(means_path, mode="w+", dtype=np.float64, shape=(maximum_blocks,))
    work = np.memmap(work_path, mode="w+", dtype=np.float64, shape=(maximum_blocks,))
    block_count = 0
    pixel_rms_sum = CompensatedSum()
    block_mean_sum_squares = CompensatedSum()
    min_pixels = max(4, int(math.ceil(0.35 * side * side)))
    try:
        for counts, sums, sums_squares in _block_slab_summaries(
            data,
            mask,
            side,
            rust_plane=rust_plane,
            casa_plane=casa_plane,
            mask_plan=mask_plan,
        ):
            qualified = counts >= min_pixels
            if not np.any(qualified):
                continue
            selected_counts = counts[qualified].astype(np.float64)
            selected_means = sums[qualified] / selected_counts
            selected_pixel_rms = np.sqrt(sums_squares[qualified] / selected_counts)
            selected_count = int(selected_means.size)
            means[block_count : block_count + selected_count] = selected_means
            block_count += selected_count
            pixel_rms_sum.add(float(np.sum(selected_pixel_rms)))
            block_mean_sum_squares.add(float(np.sum(selected_means * selected_means)))
        means.flush()
        if block_count < 3:
            return None
        pixel_rms_mean = pixel_rms_sum.value() / block_count
        block_mean_rms = math.sqrt(block_mean_sum_squares.value() / block_count)
        robust_center = _memmap_median(means, block_count)
        _fill_memmap_transform(work, means, block_count, lambda values: np.abs(values))
        median_abs_block_mean = _memmap_median(work, block_count)
        maximum_deviation = _fill_memmap_transform(
            work,
            means,
            block_count,
            lambda values: np.abs(values - robust_center),
        )
        robust_sigma = 1.4826 * _memmap_median(work, block_count)
        max_robust_z = None
        if robust_sigma > 0.0 and np.isfinite(robust_sigma):
            max_robust_z = maximum_deviation / robust_sigma
        independent_beams = (float(side) / max(1.0, float(beam_side))) ** 2
        return {
            "beam_width_multiplier": finite_float(multiplier),
            "block_side_pixels": int(side),
            "approx_independent_beams_per_block": finite_float(independent_beams),
            "n_blocks": int(block_count),
            "block_mean_rms": finite_float(block_mean_rms),
            "normalized_block_mean_rms": finite_float(block_mean_rms / flux_norm)
            if flux_norm
            else None,
            "median_abs_block_mean": finite_float(median_abs_block_mean),
            "mean_pixel_rms_in_blocks": finite_float(pixel_rms_mean),
            "block_mean_rms_over_mean_pixel_rms": finite_float(
                block_mean_rms / pixel_rms_mean
            )
            if pixel_rms_mean
            else None,
            "max_block_robust_z": finite_float(max_robust_z)
            if max_robust_z is not None
            else None,
        }
    finally:
        means.flush()
        work.flush()
        del means
        del work
        if means_path.exists():
            means_path.unlink()
        if work_path.exists():
            work_path.unlink()


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


def write_review_panel(
    panel_dir,
    suffix,
    rust_data,
    casa_data,
    diff_data,
    review=None,
    display=None,
    left_label="casa-rs",
    right_label="CASA",
):
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
            panel_rust_data = (
                display["left_data"] if "left_data" in display else display["rust_data"]
            )
            panel_casa_data = (
                display["right_data"]
                if "right_data" in display
                else display["casa_data"]
            )
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
        left_label=left_label,
        right_label=right_label,
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
        left_label=left_label,
        right_label=right_label,
    )
    return {
        "status": "written",
        "path": panel_path,
        "sha256": sha256_path(panel_path),
        "left_label": left_label,
        "right_label": right_label,
        "left_and_right_color_limits": [image_vmin, image_vmax],
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
    left_label="casa-rs",
    right_label="CASA",
):
    bounds = zoom_bounds_for_planes(rust_plane, casa_plane)
    if bounds is None:
        return {
            "status": "skipped",
            "reason": "no finite nonzero support for zoom panel",
        }
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
        return {
            "status": "skipped",
            "reason": "no finite pixels for zoom panel scaling",
        }
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
        left_label=left_label,
        right_label=right_label,
    )
    return {
        "status": "written",
        "path": zoom_path,
        "sha256": sha256_path(zoom_path),
        "left_label": left_label,
        "right_label": right_label,
        "bounds": {"x_start": x0, "x_end": x1, "y_start": y0, "y_end": y1},
        "left_and_right_color_limits": [image_vmin, image_vmax],
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
    left_label="casa-rs",
    right_label="CASA",
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
    if left_label == "casa-rs":
        axes[0].set_title(f"casa-rs {product_label}")
    else:
        axes[0].set_title(f"{left_label} {product_label}")
    casa_artist = axes[1].imshow(
        casa_plane.T,
        origin="lower",
        vmin=image_vmin,
        vmax=image_vmax,
        aspect="equal",
    )
    if right_label == "CASA":
        axes[1].set_title(f"CASA {product_label}")
    else:
        axes[1].set_title(f"{right_label} {product_label}")
    diff_artist = axes[2].imshow(
        diff_plane.T,
        origin="lower",
        vmin=-diff_abs,
        vmax=diff_abs,
        cmap="coolwarm",
        aspect="equal",
    )
    if left_label == "casa-rs" and right_label == "CASA":
        axes[2].set_title(f"difference {product_label}\n(casa-rs - CASA)")
    else:
        axes[2].set_title(f"difference {product_label}\n({left_label} - {right_label})")
    for axis in axes:
        axis.set_aspect("equal", adjustable="box")
        axis.set_box_aspect(1)
    fig.colorbar(rust_artist, ax=axes[0], fraction=0.046, pad=0.04, label=value_label)
    fig.colorbar(casa_artist, ax=axes[1], fraction=0.046, pad=0.04, label=value_label)
    if left_label == "casa-rs" and right_label == "CASA":
        fig.colorbar(
            diff_artist,
            ax=axes[2],
            fraction=0.046,
            pad=0.04,
            label=f"casa-rs - CASA ({value_label})",
        )
    else:
        fig.colorbar(
            diff_artist,
            ax=axes[2],
            fraction=0.046,
            pad=0.04,
            label=f"{left_label} - {right_label} ({value_label})",
        )
    for axis in axes:
        axis.set_xticks([])
        axis.set_yticks([])
    fig.savefig(panel_path, dpi=160)
    plt.close(fig)


def sha256_path(path):
    if not os.path.isfile(path):
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def product_value_label(suffix):
    family = product_family(suffix)
    if family in {".image", ".residual"} or suffix.startswith(".image.pbcor"):
        return "Jy/beam"
    if family == ".model":
        return "Jy/pixel"
    if family == ".psf":
        return "PSF response"
    if family in {".pb", ".weight"}:
        return "relative weight"
    return "value"


def zoom_bounds_for_planes(rust_plane, casa_plane):
    if (
        rust_plane.ndim != 2
        or casa_plane.ndim != 2
        or rust_plane.shape != casa_plane.shape
    ):
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
    tool = new_image_tool()
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
    tool = new_image_tool()
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
            sampled = product(
                math.ceil(size / step) for size, step in zip(shape, stride)
            )
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
