#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Parity and allocation proofs for bounded native structure analysis."""

from __future__ import annotations

import math
import pathlib
import resource
import sys
import tempfile
import tracemalloc
import unittest
from unittest import mock

import numpy as np

from perf_harness import casa_image_compare as comparator


def _legacy_mask(suffix, rust_plane, casa_plane, base_mask):
    if suffix == ".weight":
        scale = max(
            comparator.finite_absmax(rust_plane[base_mask]),
            comparator.finite_absmax(casa_plane[base_mask]),
        )
        if scale > 0.0:
            threshold = 1.0e-3 * scale
            return (
                base_mask
                & ((np.abs(rust_plane) > threshold) | (np.abs(casa_plane) > threshold)),
                {
                    "type": "weight_union_support",
                    "threshold_fraction_of_peak": 1.0e-3,
                    "threshold": comparator.finite_float(threshold),
                },
            )
    if suffix == ".pb":
        return base_mask & (casa_plane > 0.01), {
            "type": "casa_pb_support",
            "threshold": 0.01,
        }
    if suffix == ".weight":
        return base_mask, {"type": "finite_overlap"}
    if comparator.product_family(suffix) in {".pb", ".weight"}:
        return base_mask, {
            "type": "full_finite_overlap",
            "product_family": comparator.product_family(suffix),
        }
    return base_mask, {"type": "finite_overlap"}


def _legacy_erode(mask, suffix, beam_side):
    if suffix not in {".pb", ".weight"} or beam_side <= 1:
        return mask
    radius = int(max(1, round(beam_side)))
    if mask.shape[0] <= 2 * radius or mask.shape[1] <= 2 * radius:
        return mask
    eroded = np.zeros_like(mask, dtype=bool)
    eroded[radius:-radius, radius:-radius] = mask[radius:-radius, radius:-radius]
    return eroded


def _legacy_low_order_r2(data, mask):
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
    z -= float(np.mean(z))
    total = float(np.sum(z * z))
    if total <= 0.0 or not np.isfinite(total):
        return None
    basis = np.vstack(
        [
            np.ones_like(x),
            x,
            y,
            x * x,
            x * y,
            y * y,
            x * x + y * y,
        ]
    ).T
    coefficients, *_ = np.linalg.lstsq(basis, z, rcond=None)
    residual = z - basis @ coefficients
    return 1.0 - float(np.sum(residual * residual)) / total


def _legacy_basis_fit(reference, diff, mask):
    masked_pixels = int(np.count_nonzero(mask))
    if masked_pixels < 8:
        return {"status": "insufficient_pixels"}
    if reference.ndim != 2 or min(reference.shape) < 2:
        return {
            "status": "insufficient_dimensions",
            "shape": [int(value) for value in reference.shape],
        }
    y_gradient, x_gradient = np.gradient(reference.astype(np.float64))
    finite_basis_mask = (
        mask
        & np.isfinite(reference)
        & np.isfinite(diff)
        & np.isfinite(x_gradient)
        & np.isfinite(y_gradient)
    )
    fit_pixels = int(np.count_nonzero(finite_basis_mask))
    excluded = masked_pixels - fit_pixels
    if fit_pixels < 8:
        return {
            "status": "insufficient_finite_basis_pixels",
            "masked_pixels": masked_pixels,
            "fit_pixels": fit_pixels,
            "excluded_nonfinite_basis_pixels": excluded,
        }
    diff_values = diff[finite_basis_mask].astype(np.float64)
    reference_values = reference[finite_basis_mask].astype(np.float64)
    basis = np.vstack(
        [
            reference_values,
            np.ones_like(reference_values),
            x_gradient[finite_basis_mask].astype(np.float64),
            y_gradient[finite_basis_mask].astype(np.float64),
        ]
    ).T
    coefficients, *_ = np.linalg.lstsq(basis, diff_values, rcond=None)
    residual = diff_values - basis @ coefficients
    centered = diff_values - float(np.mean(diff_values))
    total = float(np.sum(centered * centered))
    residual_sum = float(np.sum(residual * residual))
    r2 = 1.0 - residual_sum / total if total > 0.0 and np.isfinite(total) else None
    return {
        "status": "computed",
        "model": (
            "diff ~= scale*reference + offset + dx*d_reference_dx + dy*d_reference_dy"
        ),
        "masked_pixels": masked_pixels,
        "fit_pixels": fit_pixels,
        "excluded_nonfinite_basis_pixels": excluded,
        "r2": comparator.finite_float(r2),
        "diff_rms": comparator.finite_float(comparator.rms(diff_values)),
        "residual_rms": comparator.finite_float(comparator.rms(residual)),
        "coefficients": {
            "scale": comparator.finite_float(coefficients[0]),
            "offset": comparator.finite_float(coefficients[1]),
            "dx_pixels": comparator.finite_float(coefficients[2]),
            "dy_pixels": comparator.finite_float(coefficients[3]),
        },
    }


def _legacy_large_scale_power(data, mask, beam_side, min_wavelength_beams):
    if int(np.count_nonzero(mask)) < 4:
        return None
    centered = np.asarray(data, dtype=np.float64).copy()
    centered[~mask] = np.nan
    mean = float(np.nanmean(centered))
    centered = np.where(np.isfinite(centered), centered - mean, 0.0)
    spectrum = np.fft.rfft2(centered)
    power = np.abs(spectrum) ** 2
    y_frequency = np.fft.fftfreq(centered.shape[0])[:, np.newaxis]
    x_frequency = np.fft.rfftfreq(centered.shape[1])[np.newaxis, :]
    radius = np.sqrt(x_frequency * x_frequency + y_frequency * y_frequency)
    dc = radius == 0.0
    total = float(np.sum(power[~dc]))
    if total <= 0.0 or not np.isfinite(total):
        return None
    cutoff = 1.0 / max(
        1.0,
        float(beam_side) * float(min_wavelength_beams),
    )
    selected = (radius <= cutoff) & (~dc)
    return {
        "min_wavelength_beams": comparator.finite_float(min_wavelength_beams),
        "frequency_cutoff_cycles_per_pixel": comparator.finite_float(cutoff),
        "fraction": comparator.finite_float(float(np.sum(power[selected])) / total),
    }


def _legacy_block_metric(data, mask, side, beam_side, flux_norm, multiplier):
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
            pixel_rms_values.append(comparator.rms(block))
    if len(means) < 3:
        return None
    mean_values = np.asarray(means, dtype=np.float64)
    pixel_rms_mean = float(np.mean(pixel_rms_values))
    block_mean_rms = comparator.rms(mean_values)
    robust_center = float(np.median(mean_values))
    robust_sigma = 1.4826 * float(np.median(np.abs(mean_values - robust_center)))
    max_robust_z = None
    if robust_sigma > 0.0 and np.isfinite(robust_sigma):
        max_robust_z = float(
            np.nanmax(np.abs(mean_values - robust_center)) / robust_sigma
        )
    independent_beams = (float(side) / max(1.0, float(beam_side))) ** 2
    return {
        "beam_width_multiplier": comparator.finite_float(multiplier),
        "block_side_pixels": int(side),
        "approx_independent_beams_per_block": comparator.finite_float(
            independent_beams
        ),
        "n_blocks": int(len(means)),
        "block_mean_rms": comparator.finite_float(block_mean_rms),
        "normalized_block_mean_rms": comparator.finite_float(block_mean_rms / flux_norm)
        if flux_norm
        else None,
        "median_abs_block_mean": comparator.finite_float(
            float(np.median(np.abs(mean_values)))
        ),
        "mean_pixel_rms_in_blocks": comparator.finite_float(pixel_rms_mean),
        "block_mean_rms_over_mean_pixel_rms": comparator.finite_float(
            block_mean_rms / pixel_rms_mean
        )
        if pixel_rms_mean
        else None,
        "max_block_robust_z": comparator.finite_float(max_robust_z)
        if max_robust_z is not None
        else None,
    }


def _legacy_block_metrics(data, mask, beam_side, flux_norm):
    scales = []
    normalized_rms_values = []
    independent_beam_counts = []
    for multiplier in [1, 2, 4, 8, 16, 32]:
        side = max(1, int(round(float(beam_side) * multiplier)))
        metric = _legacy_block_metric(
            data,
            mask,
            side,
            beam_side,
            flux_norm,
            multiplier,
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
        "decay_slope": comparator.finite_float(slope) if slope is not None else None,
    }


def _legacy_structured_metrics(suffix, rust_data, casa_data, diff_data, beam_info):
    rust_plane = comparator.display_plane(rust_data)
    casa_plane = comparator.display_plane(casa_data)
    diff_plane = comparator.display_plane(diff_data)
    base_mask = (
        np.isfinite(rust_plane) & np.isfinite(casa_plane) & np.isfinite(diff_plane)
    )
    mask, description = _legacy_mask(suffix, rust_plane, casa_plane, base_mask)
    masked_pixels = int(np.count_nonzero(mask))
    if masked_pixels == 0:
        return {
            "status": "no_masked_pixels",
            "mask": description,
            "finite_overlap": int(np.count_nonzero(base_mask)),
            "beam_info_status": beam_info.get("status")
            if isinstance(beam_info, dict)
            else None,
        }
    beam_side = 1
    if isinstance(beam_info, dict) and beam_info.get("status") == "estimated_from_psf":
        beam_side = max(1, int(beam_info.get("beam_block_side_pixels") or 1))
    analysis_mask = _legacy_erode(mask, suffix, beam_side)
    if not np.any(analysis_mask):
        analysis_mask = mask
    diff_values = diff_plane[analysis_mask]
    casa_values = casa_plane[analysis_mask]
    flux_norm = comparator.robust_product_scale(casa_values)
    diff_rms = comparator.rms(diff_values)
    normalized_diff_rms = diff_rms / flux_norm if flux_norm else None
    if comparator.non_spatial_product(suffix):
        classification = comparator.non_spatial_difference_classification(
            normalized_diff_rms
        )
        review = comparator.structured_difference_review(
            suffix=suffix,
            classification=classification,
            normalized_diff_rms=normalized_diff_rms,
            low_order_r2=None,
            large_scale_power=None,
            block_decay_slope=None,
        )
        return {
            "status": "computed",
            "mask": description,
            "masked_pixels": masked_pixels,
            "analysis_pixels": int(np.count_nonzero(analysis_mask)),
            "beam_block_side_pixels": int(beam_side),
            "normalization": {
                "type": "casa_support_rms_or_peak",
                "value": comparator.finite_float(flux_norm),
            },
            "diff_rms": comparator.finite_float(diff_rms),
            "normalized_diff_rms": comparator.finite_float(normalized_diff_rms),
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
    low_order_r2 = _legacy_low_order_r2(diff_plane, analysis_mask)
    large_scale_power = _legacy_large_scale_power(
        diff_plane,
        analysis_mask,
        beam_side,
        8.0,
    )
    basis_fit = _legacy_basis_fit(casa_plane, diff_plane, analysis_mask)
    block_metrics = _legacy_block_metrics(
        diff_plane,
        analysis_mask,
        beam_side,
        flux_norm,
    )
    classification = comparator.structured_difference_classification(
        normalized_diff_rms=normalized_diff_rms,
        low_order_r2=low_order_r2,
        large_scale_power=large_scale_power,
        block_decay_slope=block_metrics["decay_slope"],
    )
    review = comparator.structured_difference_review(
        suffix=suffix,
        classification=classification,
        normalized_diff_rms=normalized_diff_rms,
        low_order_r2=low_order_r2,
        large_scale_power=large_scale_power,
        block_decay_slope=block_metrics["decay_slope"],
    )
    return {
        "status": "computed",
        "mask": description,
        "masked_pixels": masked_pixels,
        "analysis_pixels": int(np.count_nonzero(analysis_mask)),
        "beam_block_side_pixels": int(beam_side),
        "normalization": {
            "type": "casa_support_rms_or_peak",
            "value": comparator.finite_float(flux_norm),
        },
        "diff_rms": comparator.finite_float(diff_rms),
        "normalized_diff_rms": comparator.finite_float(normalized_diff_rms),
        "low_order_r2_quadratic": comparator.finite_float(low_order_r2),
        "large_scale_power_fraction": large_scale_power,
        "scale_offset_gradient_fit": basis_fit,
        "beam_block_rms_by_scale": block_metrics["scales"],
        "block_rms_decay_slope_vs_independent_beams": block_metrics["decay_slope"],
        "classification": classification,
        "review": review,
    }


def _sample_planes(shape):
    y, x = np.indices(shape, dtype=np.float64)
    casa = 2.0 + 0.03 * x - 0.02 * y + 0.2 * np.sin(x / 3.0)
    diff = 2.0e-3 * (
        0.2 * x / max(1, shape[1] - 1)
        + 0.3 * y / max(1, shape[0] - 1)
        + np.sin(2.0 * np.pi * x / max(2, shape[1]))
        * np.cos(2.0 * np.pi * y / max(2, shape[0]))
    )
    return casa + diff, casa, diff


def _beam_info(side):
    return {
        "status": "estimated_from_psf",
        "coordinate_domain": "native_direction_pixels",
        "beam_block_side_pixels": side,
    }


class BoundedStructureParityTests(unittest.TestCase):
    def assert_payload_close(self, expected, actual, path="payload"):
        if isinstance(expected, dict):
            self.assertIsInstance(actual, dict, path)
            self.assertEqual(set(expected), set(actual), path)
            for key in expected:
                self.assert_payload_close(expected[key], actual[key], f"{path}.{key}")
            return
        if isinstance(expected, list):
            self.assertIsInstance(actual, list, path)
            self.assertEqual(len(expected), len(actual), path)
            for index, (left, right) in enumerate(zip(expected, actual)):
                self.assert_payload_close(left, right, f"{path}[{index}]")
            return
        if isinstance(expected, (float, np.floating)):
            self.assertIsInstance(actual, (float, int), path)
            self.assertTrue(
                math.isclose(
                    float(expected), float(actual), rel_tol=5.0e-10, abs_tol=5.0e-12
                ),
                f"{path}: expected {expected!r}, got {actual!r}",
            )
            return
        self.assertEqual(expected, actual, path)

    def assert_complete_parity(self, suffix, rust, casa, diff, beam_side):
        beam_info = _beam_info(beam_side)
        expected = _legacy_structured_metrics(
            suffix,
            rust,
            casa,
            diff,
            beam_info,
        )
        with tempfile.TemporaryDirectory() as temporary:
            actual = comparator.structured_difference_metrics(
                suffix,
                rust,
                casa,
                diff,
                beam_info,
                scratch_root=pathlib.Path(temporary),
            )
        self.assert_payload_close(expected, actual)
        return actual

    def test_complete_payload_parity_for_odd_even_masked_and_nonfinite_planes(self):
        # The 16-row case puts y-frequency 1/16 exactly on the beam=2,
        # eight-beam wavelength cutoff and locks the legacy sqrt comparison.
        for shape in [(19, 23), (16, 18)]:
            with self.subTest(shape=shape):
                rust, casa, diff = _sample_planes(shape)
                rust[1, 2] = np.nan
                diff[1, 2] = np.nan
                casa[3, 4] = np.inf
                diff[3, 4] = np.nan
                diff[5, 6] = np.nan
                result = self.assert_complete_parity(
                    ".image",
                    rust,
                    casa,
                    diff,
                    2,
                )
                self.assertEqual("finite_overlap", result["mask"]["type"])
                self.assertEqual(shape[0] * shape[1] - 3, result["masked_pixels"])

    def test_complete_payload_parity_for_exact_pb_support_and_erosion(self):
        shape = (25, 27)
        rust, casa, diff = _sample_planes(shape)
        casa[8:12, 9:14] = 0.0
        rust = casa + diff
        result = self.assert_complete_parity(".pb", rust, casa, diff, 2)
        self.assertEqual("casa_pb_support", result["mask"]["type"])
        self.assertLess(result["analysis_pixels"], result["masked_pixels"])

    def test_complete_payload_parity_for_exact_weight_union_and_erosion(self):
        shape = (25, 27)
        y, x = np.indices(shape, dtype=np.float64)
        casa = np.full(shape, 1.0e-6)
        casa[3:22, 4:23] = 1.0 + 0.01 * x[3:22, 4:23]
        rust = casa.copy()
        rust[10:13, 1:4] = 0.5
        diff = rust - casa + 1.0e-4 * np.sin(x + y)
        result = self.assert_complete_parity(".weight", rust, casa, diff, 2)
        self.assertEqual("weight_union_support", result["mask"]["type"])
        self.assertLess(result["analysis_pixels"], result["masked_pixels"])

    def test_complete_payload_parity_for_empty_erosion_fallback(self):
        shape = (17, 19)
        y, x = np.indices(shape, dtype=np.float64)
        casa = np.zeros(shape, dtype=np.float64)
        border = (y < 2) | (y >= shape[0] - 2) | (x < 2) | (x >= shape[1] - 2)
        casa[border] = 1.0
        diff = 1.0e-3 * np.sin(x + 2.0 * y)
        rust = casa + diff
        result = self.assert_complete_parity(".pb", rust, casa, diff, 2)
        self.assertEqual(result["masked_pixels"], result["analysis_pixels"])

    def test_exact_zero_peak_weight_falls_through_then_erodes(self):
        shape = (18, 20)
        rust = np.zeros(shape, dtype=np.float64)
        casa = np.zeros(shape, dtype=np.float64)
        diff = np.zeros(shape, dtype=np.float64)
        result = self.assert_complete_parity(".weight", rust, casa, diff, 2)
        self.assertEqual({"type": "finite_overlap"}, result["mask"])
        self.assertEqual((18 - 4) * (20 - 4), result["analysis_pixels"])

    def test_taylor_pb_and_weight_keep_full_overlap_without_erosion(self):
        shape = (18, 20)
        y, x = np.indices(shape, dtype=np.float64)
        casa = np.zeros(shape, dtype=np.float64)
        casa[5:13, 6:14] = 1.0
        diff = 2.0e-4 * np.cos(x + y)
        rust = casa + diff
        for suffix, family in [(".pb.tt0", ".pb"), (".weight.tt0", ".weight")]:
            with self.subTest(suffix=suffix):
                result = self.assert_complete_parity(suffix, rust, casa, diff, 3)
                self.assertEqual(
                    {"type": "full_finite_overlap", "product_family": family},
                    result["mask"],
                )
                self.assertEqual(shape[0] * shape[1], result["analysis_pixels"])

    def test_vlass_native_shape_has_an_explicit_bounded_storage_plan(self):
        plan = comparator.structure_analysis_storage_plan((12_150, 12_150))
        self.assertEqual([12_150, 12_150], plan["shape"])
        self.assertEqual(147_622_500, plan["native_pixels"])
        self.assertEqual(
            comparator.STRUCTURE_WORKING_BYTES,
            plan["resident_working_budget_bytes"],
        )
        self.assertEqual(1_181_174_400, plan["fft_intermediate_complex128_bytes"])
        self.assertEqual(2_361_960_000, plan["maximum_block_stat_float64_bytes"])
        self.assertEqual(0, plan["full_plane_ram_arrays"])
        self.assertLess(
            plan["resident_working_budget_bytes"],
            plan["native_pixels"] * np.dtype(np.float64).itemsize,
        )

    def test_multi_million_pixel_memmaps_never_allocate_a_native_plane(self):
        shape = (1024, 2048)
        maximum_constructor_elements = 600_000
        observed_constructor_elements = []

        def guarded_constructor(original):
            def call(shape_value, *args, **kwargs):
                if isinstance(shape_value, (int, np.integer)):
                    elements = int(shape_value)
                else:
                    elements = math.prod(int(value) for value in shape_value)
                observed_constructor_elements.append(elements)
                if elements > maximum_constructor_elements:
                    raise AssertionError(
                        f"resident ndarray allocation scales with native plane: {elements}"
                    )
                return original(shape_value, *args, **kwargs)

            return call

        original_empty = np.empty
        original_zeros = np.zeros
        original_ones = np.ones
        original_full = np.full
        original_lstsq = np.linalg.lstsq

        def bounded_lstsq(design, target, *args, **kwargs):
            self.assertLessEqual(design.shape[0], 7)
            self.assertLessEqual(design.shape[1], 7)
            self.assertLessEqual(target.size, 7)
            return original_lstsq(design, target, *args, **kwargs)

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            rust = np.memmap(
                root / "rust.f64", mode="w+", dtype=np.float64, shape=shape
            )
            casa = np.memmap(
                root / "casa.f64", mode="w+", dtype=np.float64, shape=shape
            )
            diff = np.memmap(
                root / "diff.f64", mode="w+", dtype=np.float64, shape=shape
            )
            x = np.arange(shape[1], dtype=np.float64)[np.newaxis, :]
            for start in range(0, shape[0], 32):
                end = min(shape[0], start + 32)
                y = np.arange(start, end, dtype=np.float64)[:, np.newaxis]
                casa_chunk = 1.0 + 0.1 * np.sin(x / 31.0) + 0.05 * np.cos(y / 17.0)
                diff_chunk = 1.0e-4 * np.sin(x / 47.0 + y / 29.0)
                casa[start:end] = casa_chunk
                diff[start:end] = diff_chunk
                rust[start:end] = casa_chunk + diff_chunk
            rust.flush()
            casa.flush()
            diff.flush()

            rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            tracemalloc.start()
            with (
                mock.patch.object(np, "empty", guarded_constructor(original_empty)),
                mock.patch.object(np, "zeros", guarded_constructor(original_zeros)),
                mock.patch.object(np, "ones", guarded_constructor(original_ones)),
                mock.patch.object(np, "full", guarded_constructor(original_full)),
                mock.patch.object(
                    np, "indices", side_effect=AssertionError("np.indices")
                ),
                mock.patch.object(
                    np, "gradient", side_effect=AssertionError("np.gradient")
                ),
                mock.patch.object(
                    np.fft,
                    "rfft2",
                    side_effect=AssertionError("np.fft.rfft2"),
                ),
                mock.patch.object(np.linalg, "lstsq", side_effect=bounded_lstsq),
            ):
                result = comparator.structured_difference_metrics(
                    ".image",
                    rust,
                    casa,
                    diff,
                    _beam_info(3),
                    scratch_root=root,
                )
            _, traced_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        rss_scale = 1 if sys.platform == "darwin" else 1024
        rss_growth_bytes = max(0, rss_after - rss_before) * rss_scale
        self.assertEqual("computed", result["status"])
        self.assertEqual(shape[0] * shape[1], result["analysis_pixels"])
        self.assertTrue(observed_constructor_elements)
        self.assertLessEqual(
            max(observed_constructor_elements),
            maximum_constructor_elements,
        )
        self.assertLess(traced_peak, 96 * 1024 * 1024)
        self.assertLess(rss_growth_bytes, 192 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
