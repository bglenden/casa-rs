#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Evaluate the VLASS scientific-correctness floor from frozen products.

This tool does not run CASA, open a MeasurementSet, or execute imaging.  It
binds an existing full-array comparison and its retained disk-backed planes,
then evaluates source, noise, dynamic-range, coherent-structure, and stable
spectral-index checks.  The output receipt is immutable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np


EXPECTED_PRODUCTS = (
    ".alpha",
    ".alpha.error",
    ".image.tt0",
    ".image.tt1",
    ".mask",
    ".model.tt0",
    ".model.tt1",
    ".pb.tt0",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".weight.tt0",
    ".weight.tt1",
    ".weight.tt2",
)
REVIEW_PLANE_PRODUCTS = (
    ".alpha",
    ".alpha.error",
    ".image.tt0",
    ".image.tt1",
    ".residual.tt0",
    ".residual.tt1",
)
COHERENT_PRODUCTS = (
    ".image.tt0",
    ".image.tt1",
    ".residual.tt0",
    ".residual.tt1",
)
CONTRACT = {
    "numerical_diff_rms_over_reference_rms_max": 1.0e-3,
    "numerical_diff_abs_max_over_reference_peak_max": 5.0e-3,
    "source_peak_relative_difference_max": 1.0e-3,
    "source_integrated_flux_relative_difference_max": 1.0e-3,
    "source_centroid_distance_pixels_max": 5.0e-2,
    "source_moment_centroid_distance_pixels_max": 5.0e-2,
    "source_covariance_relative_frobenius_max": 1.0e-3,
    "noise_location_over_reference_sigma_max": 1.0e-3,
    "noise_scale_relative_difference_max": 1.0e-3,
    "noise_quantile_delta_over_reference_sigma_max": 1.0e-3,
    "dynamic_range_relative_difference_max": 1.0e-3,
    "coherent_block_rms_over_reference_noise_max": 1.0e-3,
    "coherent_block_rms_over_reference_signal_max": 1.0e-4,
    # A per-pixel guard catches a localized artifact, not ordinary sub-noise
    # arithmetic scatter.  Beam-scale and larger coherence have independent,
    # much tighter RMS limits above.
    "difference_abs_max_over_reference_noise_max": 5.0e-2,
    "alpha_signal_to_noise_min": 5.0,
    "alpha_threshold_guard_fraction": 5.0e-2,
    "alpha_boundary_guard_fraction": 1.0e-4,
    "alpha_boundary_fraction_max": 1.0e-5,
    "alpha_stable_rms_difference_max": 1.0e-2,
    "alpha_stable_abs_max_difference_max": 5.0e-2,
    "alpha_stable_correlation_min": 0.9999,
    "off_source_sample_limit": 1_048_576,
}
QUANTILES = (0.001, 0.01, 0.16, 0.5, 0.84, 0.99, 0.999)


class ReviewError(ValueError):
    """The frozen evidence is incomplete or violates the review protocol."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def structure_product_workspace(workspace_root: Path, suffix: str) -> Path:
    safe_suffix = suffix.strip(".").replace(".", "_") or "image"
    digest = hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:12]
    return workspace_root / f"{safe_suffix}-{digest}"


def finite_float(value: float | np.floating[Any]) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ReviewError("scientific review produced a non-finite metric")
    return result


def relative_difference(left: float, right: float) -> float:
    scale = abs(float(right))
    if scale == 0.0:
        return 0.0 if float(left) == 0.0 else math.inf
    return abs(float(left) - float(right)) / scale


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ReviewError(f"expected a JSON object: {path}")
    return value


def shape_from_comparison(comparison: dict[str, Any]) -> tuple[int, int]:
    try:
        shape = comparison["products"][".image.tt0"]["full_array"]["shape"]
    except (KeyError, TypeError) as error:
        raise ReviewError("comparison lacks authoritative image shape") from error
    if (
        not isinstance(shape, list)
        or len(shape) < 2
        or not all(isinstance(value, int) and value > 0 for value in shape[:2])
    ):
        raise ReviewError("comparison image shape is invalid")
    return int(shape[0]), int(shape[1])


def validate_comparison(
    comparison: dict[str, Any],
    comparison_input: dict[str, Any],
) -> dict[str, Any]:
    failures: list[str] = []
    requested_products = comparison.get("requested_products")
    if requested_products != list(EXPECTED_PRODUCTS):
        failures.append("requested 19-product inventory is not exact")
    if comparison_input.get("products") != list(EXPECTED_PRODUCTS):
        failures.append("comparison input 19-product inventory is not exact")
    if comparison.get("comparison_mode") != "full":
        failures.append("comparison is not full-array")
    if comparison.get("request_sha256") != comparison_input.get("request_sha256"):
        failures.append("comparison request hash does not match its input")
    if comparison.get("product_inventory", {}).get("status") != "matched":
        failures.append("product inventory differs")
    if not comparison.get("require_exact_product_inventory"):
        failures.append("exact product inventory was not required")
    if not comparison.get("require_metadata_parity"):
        failures.append("metadata parity was not required")

    products = comparison.get("products")
    if not isinstance(products, dict) or set(products) != set(EXPECTED_PRODUCTS):
        failures.append("comparison product result inventory is not exact")
        products = {}
    metadata_failures: list[str] = []
    numerical_failures: list[str] = []
    numerical_metrics: dict[str, Any] = {}
    for suffix in EXPECTED_PRODUCTS:
        product = products.get(suffix, {})
        if product.get("metadata", {}).get("parity") is not True:
            metadata_failures.append(suffix)
        status = product.get("status")
        allowed_statuses = (
            {"compared", "topology_mismatch"}
            if suffix in {".alpha", ".alpha.error"}
            else {"compared"}
        )
        if status not in allowed_statuses:
            failures.append(f"{suffix} comparison status is {status!r}")
        full = product.get("full_array", {})
        if full.get("coverage_complete") is not True:
            failures.append(f"{suffix} full-array coverage is incomplete")
        rms_ratio = full.get("diff_rms_over_right_rms")
        peak_ratio = full.get("diff_abs_max_over_right_peak")
        numerical_metrics[suffix] = {
            "diff_rms_over_reference_rms": rms_ratio,
            "diff_abs_max_over_reference_peak": peak_ratio,
        }
        if not isinstance(rms_ratio, (int, float)) or (
            float(rms_ratio) > CONTRACT["numerical_diff_rms_over_reference_rms_max"]
        ):
            numerical_failures.append(f"{suffix}:rms")
        if not isinstance(peak_ratio, (int, float)) or (
            float(peak_ratio)
            > CONTRACT["numerical_diff_abs_max_over_reference_peak_max"]
        ):
            numerical_failures.append(f"{suffix}:peak")
    if metadata_failures:
        failures.append(
            "metadata parity failed for " + ", ".join(sorted(metadata_failures))
        )
    if numerical_failures:
        failures.append(
            "frozen numerical ceiling failed for "
            + ", ".join(sorted(numerical_failures))
        )
    return {
        "passed": not failures,
        "failures": failures,
        "numerical_metrics": numerical_metrics,
        "metadata_failures": metadata_failures,
    }


def open_review_planes(
    comparison: dict[str, Any],
    workspace_root: Path,
    shape: tuple[int, int],
) -> tuple[dict[str, dict[str, np.memmap]], dict[str, Any]]:
    expected_value_bytes = math.prod(shape) * np.dtype(np.float64).itemsize
    expected_coverage_bytes = math.prod(shape)
    planes: dict[str, dict[str, np.memmap]] = {}
    bindings: dict[str, Any] = {}
    for suffix in REVIEW_PLANE_PRODUCTS:
        product = comparison["products"].get(suffix, {})
        product_shape = product.get("full_array", {}).get("shape", [])
        if tuple(product_shape[:2]) != shape:
            raise ReviewError(f"{suffix} shape differs from the image shape")
        directory = structure_product_workspace(workspace_root, suffix)
        if not directory.is_dir() or directory.is_symlink():
            raise ReviewError(f"retained workspace is unavailable for {suffix}")
        files = {
            "left": directory / "left.f64",
            "right": directory / "right.f64",
            "coverage": directory / "coverage.u8",
        }
        if files["left"].stat().st_size != expected_value_bytes:
            raise ReviewError(f"{suffix} casa-rs plane size is invalid")
        if files["right"].stat().st_size != expected_value_bytes:
            raise ReviewError(f"{suffix} CASA plane size is invalid")
        if files["coverage"].stat().st_size != expected_coverage_bytes:
            raise ReviewError(f"{suffix} coverage plane size is invalid")
        coverage = np.memmap(files["coverage"], mode="r", dtype=np.uint8, shape=shape)
        if int(np.count_nonzero(coverage)) != math.prod(shape):
            raise ReviewError(f"{suffix} retained workspace coverage is incomplete")
        planes[suffix] = {
            "left": np.memmap(files["left"], mode="r", dtype=np.float64, shape=shape),
            "right": np.memmap(files["right"], mode="r", dtype=np.float64, shape=shape),
        }
        bindings[suffix] = {
            operand: {
                "path": str(path.resolve()),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for operand, path in files.items()
        }
    return planes, bindings


def sampled_off_source_values(
    left: np.memmap,
    right: np.memmap,
    source_regions: list[dict[str, Any]],
) -> tuple[np.ndarray, np.ndarray, int]:
    pixels = math.prod(left.shape)
    stride = max(
        1,
        int(math.ceil(math.sqrt(pixels / int(CONTRACT["off_source_sample_limit"])))),
    )
    sampled_left = np.array(left[::stride, ::stride], dtype=np.float64, copy=True)
    sampled_right = np.array(right[::stride, ::stride], dtype=np.float64, copy=True)
    keep = np.ones(sampled_left.shape, dtype=bool)
    xs = np.arange(0, left.shape[0], stride)
    ys = np.arange(0, left.shape[1], stride)
    for region in source_regions:
        blc = region["blc"]
        trc = region["trc"]
        x_selection = np.flatnonzero((xs >= blc[0]) & (xs <= trc[0]))
        y_selection = np.flatnonzero((ys >= blc[1]) & (ys <= trc[1]))
        if x_selection.size and y_selection.size:
            keep[
                x_selection[0] : x_selection[-1] + 1,
                y_selection[0] : y_selection[-1] + 1,
            ] = False
    keep &= np.isfinite(sampled_left) & np.isfinite(sampled_right)
    return sampled_left[keep], sampled_right[keep], stride


def distribution_statistics(values: np.ndarray) -> dict[str, Any]:
    if values.size < 2:
        raise ReviewError("off-source distribution has fewer than two samples")
    median = finite_float(np.median(values))
    deviations = np.abs(values - median)
    robust_sigma = finite_float(1.4826 * np.median(deviations))
    if robust_sigma <= 0.0:
        raise ReviewError("off-source robust noise is not positive")
    quantiles = np.quantile(values, QUANTILES)
    return {
        "count": int(values.size),
        "mean": finite_float(np.mean(values)),
        "median": median,
        "rms": finite_float(np.sqrt(np.mean(values * values))),
        "standard_deviation": finite_float(np.std(values)),
        "mad_sigma": robust_sigma,
        "quantiles": {
            f"{quantile:.3f}": finite_float(value)
            for quantile, value in zip(QUANTILES, quantiles)
        },
    }


def noise_review(
    planes: dict[str, dict[str, np.memmap]],
    source_regions: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    terms: dict[str, Any] = {}
    for term in ("tt0", "tt1"):
        suffix = f".residual.{term}"
        left_values, right_values, stride = sampled_off_source_values(
            planes[suffix]["left"],
            planes[suffix]["right"],
            source_regions,
        )
        left = distribution_statistics(left_values)
        right = distribution_statistics(right_values)
        sigma = right["mad_sigma"]
        checks = {
            "median_delta_over_reference_sigma": abs(left["median"] - right["median"])
            / sigma,
            "mad_sigma_relative_difference": relative_difference(
                left["mad_sigma"], right["mad_sigma"]
            ),
            "rms_relative_difference": relative_difference(left["rms"], right["rms"]),
            "quantile_delta_over_reference_sigma_max": max(
                abs(left["quantiles"][key] - right["quantiles"][key]) / sigma
                for key in right["quantiles"]
            ),
        }
        if (
            checks["median_delta_over_reference_sigma"]
            > CONTRACT["noise_location_over_reference_sigma_max"]
        ):
            failures.append(f"{suffix} noise median")
        if (
            checks["mad_sigma_relative_difference"]
            > CONTRACT["noise_scale_relative_difference_max"]
            or checks["rms_relative_difference"]
            > CONTRACT["noise_scale_relative_difference_max"]
        ):
            failures.append(f"{suffix} noise scale")
        if (
            checks["quantile_delta_over_reference_sigma_max"]
            > CONTRACT["noise_quantile_delta_over_reference_sigma_max"]
        ):
            failures.append(f"{suffix} noise distribution")
        terms[term] = {
            "method": ("deterministic_spatial_lattice_excluding_frozen_source_regions"),
            "sample_stride": stride,
            "left": left,
            "right": right,
            "checks": checks,
        }
    return terms, failures


def moment_statistics(
    values: np.ndarray,
    *,
    x0: int,
    y0: int,
    background: float,
    center: tuple[float, float],
    radius: float,
) -> dict[str, Any]:
    xs = np.arange(x0, x0 + values.shape[0], dtype=np.float64)[:, None]
    ys = np.arange(y0, y0 + values.shape[1], dtype=np.float64)[None, :]
    aperture = (xs - center[0]) ** 2 + (ys - center[1]) ** 2 <= radius**2
    finite = np.isfinite(values)
    weights = np.where(aperture & finite, np.maximum(values - background, 0.0), 0.0)
    weight_sum = float(np.sum(weights))
    if weight_sum <= 0.0:
        raise ReviewError("source morphology aperture has no positive weight")
    centroid_x = float(np.sum(weights * xs) / weight_sum)
    centroid_y = float(np.sum(weights * ys) / weight_sum)
    dx = xs - centroid_x
    dy = ys - centroid_y
    covariance = np.array(
        [
            [
                float(np.sum(weights * dx * dx) / weight_sum),
                float(np.sum(weights * dx * dy) / weight_sum),
            ],
            [
                float(np.sum(weights * dx * dy) / weight_sum),
                float(np.sum(weights * dy * dy) / weight_sum),
            ],
        ],
        dtype=np.float64,
    )
    eigenvalues = np.linalg.eigvalsh(covariance)
    return {
        "aperture_pixels": int(np.count_nonzero(aperture)),
        "positive_weight_pixels": int(np.count_nonzero(weights)),
        "weight_sum": finite_float(weight_sum),
        "centroid_pixels": [finite_float(centroid_x), finite_float(centroid_y)],
        "covariance_pixels_squared": covariance.tolist(),
        "minor_sigma_pixels": finite_float(math.sqrt(max(0.0, eigenvalues[0]))),
        "major_sigma_pixels": finite_float(math.sqrt(max(0.0, eigenvalues[1]))),
    }


def source_review(
    comparison: dict[str, Any],
    planes: dict[str, dict[str, np.memmap]],
    noise: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    source_results = comparison["products"][".image.tt0"].get("source_regions", [])
    if len(source_results) != 1:
        raise ReviewError("scientific review requires exactly one frozen source region")
    source = source_results[0]
    left = source["left"]
    right = source["right"]
    peak_relative = relative_difference(
        left["peak_abs"]["abs_value"], right["peak_abs"]["abs_value"]
    )
    flux_relative = relative_difference(
        left["integrated_flux"], right["integrated_flux"]
    )
    centroid_distance = math.dist(left["centroid_pixels"], right["centroid_pixels"])
    if peak_relative > CONTRACT["source_peak_relative_difference_max"]:
        failures.append("source peak")
    if flux_relative > CONTRACT["source_integrated_flux_relative_difference_max"]:
        failures.append("source integrated flux")
    if centroid_distance > CONTRACT["source_centroid_distance_pixels_max"]:
        failures.append("source centroid")

    blc = source["blc"]
    trc = source["trc"]
    source_left = np.asarray(
        planes[".image.tt0"]["left"][blc[0] : trc[0] + 1, blc[1] : trc[1] + 1],
        dtype=np.float64,
    )
    source_right = np.asarray(
        planes[".image.tt0"]["right"][blc[0] : trc[0] + 1, blc[1] : trc[1] + 1],
        dtype=np.float64,
    )
    beam_fwhm = comparison["beam_info"]["fwhm_pixels"]
    radius = 4.0 * max(float(value) for value in beam_fwhm)
    peak_center = tuple(float(value) for value in right["peak_abs"]["location"])
    left_moments = moment_statistics(
        source_left,
        x0=blc[0],
        y0=blc[1],
        background=noise["tt0"]["left"]["median"],
        center=peak_center,
        radius=radius,
    )
    right_moments = moment_statistics(
        source_right,
        x0=blc[0],
        y0=blc[1],
        background=noise["tt0"]["right"]["median"],
        center=peak_center,
        radius=radius,
    )
    moment_centroid_distance = math.dist(
        left_moments["centroid_pixels"], right_moments["centroid_pixels"]
    )
    left_covariance = np.asarray(
        left_moments["covariance_pixels_squared"], dtype=np.float64
    )
    right_covariance = np.asarray(
        right_moments["covariance_pixels_squared"], dtype=np.float64
    )
    covariance_norm = float(np.linalg.norm(right_covariance))
    covariance_relative = (
        float(np.linalg.norm(left_covariance - right_covariance)) / covariance_norm
        if covariance_norm
        else math.inf
    )
    if (
        moment_centroid_distance
        > CONTRACT["source_moment_centroid_distance_pixels_max"]
    ):
        failures.append("source moment centroid")
    if covariance_relative > CONTRACT["source_covariance_relative_frobenius_max"]:
        failures.append("source morphology covariance")
    return (
        {
            "region": {
                "id": source["id"],
                "blc": blc,
                "trc": trc,
            },
            "photometry_and_position": {
                "left": left,
                "right": right,
                "peak_relative_difference": peak_relative,
                "integrated_flux_relative_difference": flux_relative,
                "centroid_distance_pixels": centroid_distance,
            },
            "morphology": {
                "method": (
                    "positive_background_subtracted_second_moments_within_"
                    "four_reference_psf_fwhm"
                ),
                "aperture_radius_pixels": radius,
                "left": left_moments,
                "right": right_moments,
                "centroid_distance_pixels": moment_centroid_distance,
                "covariance_relative_frobenius": covariance_relative,
            },
        },
        failures,
    )


def dynamic_range_review(
    comparison: dict[str, Any],
    noise: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    terms: dict[str, Any] = {}
    for term in ("tt0", "tt1"):
        product = comparison["products"][f".image.{term}"]["full_array"]
        left_peak = float(product["left_peak_abs"]["abs_value"])
        right_peak = float(product["right_peak_abs"]["abs_value"])
        left_value = left_peak / noise[term]["left"]["mad_sigma"]
        right_value = right_peak / noise[term]["right"]["mad_sigma"]
        difference = relative_difference(left_value, right_value)
        if difference > CONTRACT["dynamic_range_relative_difference_max"]:
            failures.append(f"{term} dynamic range")
        terms[term] = {
            "left": left_value,
            "right": right_value,
            "relative_difference": difference,
        }
    return terms, failures


def coherent_difference_review(
    comparison: dict[str, Any],
    noise: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    products: dict[str, Any] = {}
    for suffix in COHERENT_PRODUCTS:
        term = "tt1" if suffix.endswith("tt1") else "tt0"
        full = comparison["products"][suffix]["full_array"]
        structure = full.get("structured_difference", {})
        scales = structure.get("beam_block_rms_by_scale", [])
        if not scales:
            raise ReviewError(f"{suffix} lacks beam-block difference evidence")
        reference_noise = noise[term]["right"]["mad_sigma"]
        reference_signal = float(full["right_peak_abs"]["abs_value"])
        scale_metrics = [
            {
                **scale,
                "block_rms_over_reference_noise": (
                    float(scale["block_mean_rms"]) / reference_noise
                ),
                "block_rms_over_reference_signal": (
                    float(scale["block_mean_rms"]) / reference_signal
                ),
            }
            for scale in scales
        ]
        max_noise = max(
            metric["block_rms_over_reference_noise"] for metric in scale_metrics
        )
        max_signal = max(
            metric["block_rms_over_reference_signal"] for metric in scale_metrics
        )
        large_scale_noise = max(
            metric["block_rms_over_reference_noise"]
            for metric in scale_metrics
            if float(metric["beam_width_multiplier"]) >= 8.0
        )
        difference_abs_max_over_noise = float(full["diff_abs_max"]) / reference_noise
        if max_noise > CONTRACT["coherent_block_rms_over_reference_noise_max"]:
            failures.append(f"{suffix} beam-scale difference over noise")
        if max_signal > CONTRACT["coherent_block_rms_over_reference_signal_max"]:
            failures.append(f"{suffix} beam-scale difference over signal")
        if (
            difference_abs_max_over_noise
            > CONTRACT["difference_abs_max_over_reference_noise_max"]
        ):
            failures.append(f"{suffix} maximum difference over noise")
        products[suffix] = {
            "reference_noise": reference_noise,
            "reference_signal_peak": reference_signal,
            "difference_abs_max_over_reference_noise": (difference_abs_max_over_noise),
            "block_rms_over_reference_noise_max": max_noise,
            "block_rms_over_reference_signal_max": max_signal,
            "large_scale_8_beams_or_more_rms_over_reference_noise_max": (
                large_scale_noise
            ),
            "large_scale_power_fraction": structure.get("large_scale_power_fraction"),
            "low_order_r2_quadratic": structure.get("low_order_r2_quadratic"),
            "scales": scale_metrics,
        }
    return products, failures


def correlation(left: np.ndarray, right: np.ndarray) -> float:
    if left.size < 2:
        raise ReviewError("stable alpha domain has fewer than two pixels")
    left_centered = left - np.mean(left)
    right_centered = right - np.mean(right)
    denominator = math.sqrt(
        float(np.sum(left_centered * left_centered))
        * float(np.sum(right_centered * right_centered))
    )
    if denominator == 0.0:
        return 1.0 if np.array_equal(left, right) else 0.0
    return finite_float(float(np.sum(left_centered * right_centered)) / denominator)


def alpha_review(
    comparison: dict[str, Any],
    planes: dict[str, dict[str, np.memmap]],
    noise: dict[str, Any],
    alpha_threshold: float,
) -> tuple[dict[str, Any], list[str], np.ndarray]:
    failures: list[str] = []
    alpha_topology = comparison["products"][".alpha"]["full_array"]["topology"]
    error_topology = comparison["products"][".alpha.error"]["full_array"]["topology"]
    alpha_samples = alpha_topology.get("mask_mismatch_samples", [])
    error_samples = error_topology.get("mask_mismatch_samples", [])
    alpha_locations = [sample["location"][:2] for sample in alpha_samples]
    error_locations = [sample["location"][:2] for sample in error_samples]
    if alpha_locations != error_locations:
        failures.append("alpha and alpha-error boundary locations differ")
    mismatch_count = int(alpha_topology["mask_mismatch_count"])
    error_mismatch_count = int(error_topology["mask_mismatch_count"])
    if mismatch_count != len(alpha_samples):
        failures.append("alpha boundary evidence does not enumerate every mismatch")
    if error_mismatch_count != len(error_samples):
        failures.append(
            "alpha-error boundary evidence does not enumerate every mismatch"
        )
    common_count = int(
        comparison["products"][".alpha"]["full_array"]["comparison_domain_count"]
    )
    mismatch_fraction = mismatch_count / common_count
    boundary_values: list[dict[str, Any]] = []
    boundary_max_relative_distance = 0.0
    for location in alpha_locations:
        x, y = (int(value) for value in location)
        left_value = float(planes[".image.tt0"]["left"][x, y])
        right_value = float(planes[".image.tt0"]["right"][x, y])
        relative_distance = max(
            abs(left_value - alpha_threshold) / alpha_threshold,
            abs(right_value - alpha_threshold) / alpha_threshold,
        )
        boundary_max_relative_distance = max(
            boundary_max_relative_distance, relative_distance
        )
        boundary_values.append(
            {
                "location": [x, y],
                "left_image_tt0": left_value,
                "right_image_tt0": right_value,
                "max_relative_distance_from_cutoff": relative_distance,
            }
        )
    if mismatch_count != error_mismatch_count:
        failures.append("alpha and alpha-error mismatch counts differ")
    if mismatch_fraction > CONTRACT["alpha_boundary_fraction_max"]:
        failures.append("alpha boundary mismatch fraction")
    if boundary_max_relative_distance > CONTRACT["alpha_boundary_guard_fraction"]:
        failures.append("alpha topology mismatch is not confined to cutoff boundary")

    stable_cutoff = max(
        alpha_threshold * (1.0 + CONTRACT["alpha_threshold_guard_fraction"]),
        noise["tt0"]["right"]["mad_sigma"] * CONTRACT["alpha_signal_to_noise_min"],
    )
    image_left = planes[".image.tt0"]["left"]
    image_right = planes[".image.tt0"]["right"]
    alpha_left = planes[".alpha"]["left"]
    alpha_right = planes[".alpha"]["right"]
    error_left = planes[".alpha.error"]["left"]
    error_right = planes[".alpha.error"]["right"]
    stable = (
        (image_left > stable_cutoff)
        & (image_right > stable_cutoff)
        & np.isfinite(alpha_left)
        & np.isfinite(alpha_right)
        & np.isfinite(error_left)
        & np.isfinite(error_right)
    )
    stable_count = int(np.count_nonzero(stable))
    minimum_pixels = max(
        2,
        int(math.ceil(float(comparison["beam_info"]["beam_area_pixels"]))),
    )
    if stable_count < minimum_pixels:
        failures.append("stable alpha domain is smaller than one reference beam")
    stable_products: dict[str, Any] = {}
    for suffix in (".alpha", ".alpha.error"):
        left_values = np.asarray(planes[suffix]["left"][stable], dtype=np.float64)
        right_values = np.asarray(planes[suffix]["right"][stable], dtype=np.float64)
        differences = left_values - right_values
        rms_difference = finite_float(
            math.sqrt(float(np.mean(differences * differences)))
        )
        abs_max_difference = finite_float(np.max(np.abs(differences)))
        correlation_value = correlation(left_values, right_values)
        if rms_difference > CONTRACT["alpha_stable_rms_difference_max"]:
            failures.append(f"{suffix} stable-domain RMS")
        if abs_max_difference > CONTRACT["alpha_stable_abs_max_difference_max"]:
            failures.append(f"{suffix} stable-domain maximum")
        if correlation_value < CONTRACT["alpha_stable_correlation_min"]:
            failures.append(f"{suffix} stable-domain correlation")
        stable_products[suffix] = {
            "count": stable_count,
            "left_rms": finite_float(
                math.sqrt(float(np.mean(left_values * left_values)))
            ),
            "right_rms": finite_float(
                math.sqrt(float(np.mean(right_values * right_values)))
            ),
            "difference_rms": rms_difference,
            "difference_abs_max": abs_max_difference,
            "correlation": correlation_value,
        }
    return (
        {
            "cutoff_boundary": {
                "alpha_threshold": alpha_threshold,
                "alpha_mismatch_count": mismatch_count,
                "alpha_error_mismatch_count": error_mismatch_count,
                "comparison_domain_count": common_count,
                "mismatch_fraction": mismatch_fraction,
                "maximum_relative_distance_from_cutoff": (
                    boundary_max_relative_distance
                ),
                "samples": boundary_values,
            },
            "stable_science_domain": {
                "method": (
                    "common_positive_image_tt0_above_max_of_5sigma_and_"
                    "five_percent_cutoff_guard"
                ),
                "minimum_signal_to_noise": CONTRACT["alpha_signal_to_noise_min"],
                "cutoff_guard_fraction": CONTRACT["alpha_threshold_guard_fraction"],
                "image_tt0_cutoff": stable_cutoff,
                "minimum_pixels": minimum_pixels,
                "count": stable_count,
                "products": stable_products,
            },
        },
        failures,
        stable,
    )


def render_panel(
    output: Path,
    planes: dict[str, dict[str, np.memmap]],
    source: dict[str, Any],
    stable_alpha: np.ndarray,
    status: str,
    workload_id: str,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    blc = source["region"]["blc"]
    trc = source["region"]["trc"]
    selection = (
        slice(blc[0], trc[0] + 1),
        slice(blc[1], trc[1] + 1),
    )
    rows = (
        (".image.tt0", "restored image TT0", "Jy/beam"),
        (".residual.tt0", "residual TT0", "Jy/beam"),
        (".alpha", "stable-domain alpha", "spectral index"),
    )
    fig, axes = plt.subplots(3, 3, figsize=(15, 14), constrained_layout=True)
    fig.suptitle(
        f"VLASS scientific-floor review ({workload_id}): {status}",
        fontsize=15,
    )
    for row, (suffix, title, unit) in enumerate(rows):
        left = np.array(planes[suffix]["left"][selection], dtype=np.float64)
        right = np.array(planes[suffix]["right"][selection], dtype=np.float64)
        if suffix == ".alpha":
            stable = stable_alpha[selection]
            left = np.where(stable, left, np.nan)
            right = np.where(stable, right, np.nan)
        difference = left - right
        finite_reference = np.concatenate(
            (left[np.isfinite(left)], right[np.isfinite(right)])
        )
        if finite_reference.size == 0:
            value_min, value_max = -1.0, 1.0
        elif suffix == ".residual.tt0":
            peak = float(np.max(np.abs(finite_reference)))
            value_min, value_max = -peak, peak
        else:
            value_min = float(np.min(finite_reference))
            value_max = float(np.max(finite_reference))
            if value_min == value_max:
                value_min -= 1.0
                value_max += 1.0
        finite_difference = difference[np.isfinite(difference)]
        difference_peak = (
            float(np.max(np.abs(finite_difference))) if finite_difference.size else 1.0
        )
        if difference_peak == 0.0:
            difference_peak = 1.0
        displays = (
            (left, "casa-rs", value_min, value_max, "viridis"),
            (right, "CASA", value_min, value_max, "viridis"),
            (
                difference,
                "casa-rs − CASA",
                -difference_peak,
                difference_peak,
                "RdBu_r",
            ),
        )
        for column, (values, label, vmin, vmax, cmap) in enumerate(displays):
            artist = axes[row, column].imshow(
                values.T,
                origin="lower",
                interpolation="nearest",
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
            )
            axes[row, column].set_title(f"{title}: {label}")
            axes[row, column].set_xlabel(f"x pixel + {blc[0]}")
            axes[row, column].set_ylabel(f"y pixel + {blc[1]}")
            fig.colorbar(artist, ax=axes[row, column], label=unit)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def build_review(
    *,
    workload_id: str,
    comparison_path: Path,
    comparison_input_path: Path,
    run_log_path: Path,
    workspace_root: Path,
    alpha_threshold: float,
    panel_path: Path | None,
) -> dict[str, Any]:
    if not workload_id.strip():
        raise ReviewError("workload id must not be empty")
    if alpha_threshold <= 0.0 or not math.isfinite(alpha_threshold):
        raise ReviewError("alpha threshold must be finite and positive")
    comparison = load_json(comparison_path)
    comparison_input = load_json(comparison_input_path)
    comparison_gate = validate_comparison(comparison, comparison_input)
    shape = shape_from_comparison(comparison)
    planes, plane_bindings = open_review_planes(comparison, workspace_root, shape)
    source_regions = comparison.get("source_regions", [])
    if not isinstance(source_regions, list) or not source_regions:
        raise ReviewError("comparison has no frozen source regions")

    noise, noise_failures = noise_review(planes, source_regions)
    source, source_failures = source_review(comparison, planes, noise)
    dynamic_range, dynamic_range_failures = dynamic_range_review(comparison, noise)
    coherent, coherent_failures = coherent_difference_review(comparison, noise)
    alpha, alpha_failures, stable_alpha = alpha_review(
        comparison,
        planes,
        noise,
        alpha_threshold,
    )
    failures = [
        *comparison_gate["failures"],
        *source_failures,
        *noise_failures,
        *dynamic_range_failures,
        *coherent_failures,
        *alpha_failures,
    ]
    status = "passed" if not failures else "failed"
    panel_binding = None
    if panel_path is not None:
        if panel_path.exists():
            raise ReviewError(f"refusing to overwrite panel: {panel_path}")
        render_panel(
            panel_path,
            planes,
            source,
            stable_alpha,
            status,
            workload_id,
        )
        panel_binding = {
            "path": str(panel_path.resolve()),
            "bytes": panel_path.stat().st_size,
            "sha256": sha256_file(panel_path),
        }

    return {
        "schema_version": 1,
        "kind": "vlass_scientific_floor_review",
        "status": status,
        "decision": "promote" if status == "passed" else "hold",
        "failures": failures,
        "scope": {
            "workload": workload_id,
            "evidence_only": True,
            "runs_casa": False,
            "runs_imaging": False,
            "shape": list(shape),
            "products": list(EXPECTED_PRODUCTS),
        },
        "contract": CONTRACT,
        "input": {
            "reviewer_source": {
                "path": str(Path(__file__).resolve()),
                "sha256": sha256_file(Path(__file__).resolve()),
            },
            "comparison": {
                "path": str(comparison_path.resolve()),
                "sha256": sha256_file(comparison_path),
            },
            "comparison_input": {
                "path": str(comparison_input_path.resolve()),
                "sha256": sha256_file(comparison_input_path),
            },
            "run_log": {
                "path": str(run_log_path.resolve()),
                "sha256": sha256_file(run_log_path),
            },
            "structure_workspace": str(workspace_root.resolve()),
            "planes": plane_bindings,
        },
        "gates": {
            "inventory_metadata_and_frozen_numerical_ceilings": (
                comparison_gate["passed"]
            ),
            "source_photometry_position_and_morphology": not source_failures,
            "residual_noise_and_distribution": not noise_failures,
            "dynamic_range": not dynamic_range_failures,
            "beam_and_larger_scale_difference_amplitude": not coherent_failures,
            "stable_alpha_and_cutoff_boundary": not alpha_failures,
        },
        "metrics": {
            "comparison": comparison_gate,
            "source": source,
            "noise": noise,
            "dynamic_range": dynamic_range,
            "coherent_difference": coherent,
            "alpha": alpha,
        },
        "visual_panel": panel_binding,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload-id", required=True)
    parser.add_argument("--comparison", required=True, type=Path)
    parser.add_argument("--comparison-input", required=True, type=Path)
    parser.add_argument("--run-log", required=True, type=Path)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--alpha-threshold", required=True, type=float)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--panel", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"refusing to overwrite receipt: {args.output}")
    receipt = build_review(
        workload_id=args.workload_id,
        comparison_path=args.comparison,
        comparison_input_path=args.comparison_input,
        run_log_path=args.run_log,
        workspace_root=args.workspace,
        alpha_threshold=args.alpha_threshold,
        panel_path=args.panel,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"status": receipt["status"], "output": str(args.output)}))
    if receipt["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
