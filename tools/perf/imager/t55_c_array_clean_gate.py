#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Versioned, fail-closed science assessment of C-array deep-CLEAN channels.

Nonlinear agreement outliers require comparison-bound review; they never pass
unattended. Structural, deterministic-product, and convergence checks remain
hard. This is the C-array cube gate, not full T55 acceptance.
"""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

from perf_harness.tolerances import scientific_beam_metrics


CHANNELS = (0, 256, 511)
RULE = "t55-c-array-deep-clean-science-v3"
PRODUCTS = {".image", ".mask", ".model", ".pb", ".psf", ".residual", ".sumwt"}
ASSESSMENT_PRODUCTS = PRODUCTS - {".model"}
DIAGNOSTIC_ONLY = {
    ".model.diff_rms_over_right_rms",
    ".residual.diff_rms_over_right_rms",
}
STRICT_PLANE_PRODUCTS = (".image", ".mask", ".pb", ".psf", ".sumwt")
REVIEWABLE_COMPARISON = {".image.diff_rms_over_right_rms"}
REVIEWABLE_SCIENCE = {
    "residual_difference_rms_over_casa",
    "residual_difference_max_over_casa_rms",
    "restored_component_difference_rms_over_casa",
}
THRESHOLD_JY = 5e-4
CLEAN_RADIUS_ARCSEC = 27.5
CELL_ARCSEC = 0.06


def rms(values):
    return float(np.sqrt(np.mean(np.square(values, dtype=np.float64))))


def strict_comparison_checks(comparison):
    if comparison["comparison_mode"] != "full":
        raise ValueError("comparison is not full-array")
    if not all(comparison[key] for key in ("require_direction_wcs_parity",
                                           "require_metadata_parity",
                                           "require_exact_product_inventory")):
        raise ValueError("structural/metadata parity requirement is disabled")
    if comparison["product_inventory"]["status"] != "matched":
        raise ValueError("seven-product inventory differs")
    if set(comparison["products"]) != PRODUCTS:
        raise ValueError("seven-product comparison is incomplete")
    if any(product["status"] != "compared" or not product["full_array"]["coverage_complete"]
           or not product["topology_parity"]
           or not product["full_array"]["topology"]["mask_equal"]
           or not product["full_array"]["topology"]["finite_equal"]
           or any(product["full_array"]["topology"][side][kind]
                  for side in ("left_nonfinite", "right_nonfinite")
                  for kind in ("nan", "positive_infinity", "negative_infinity"))
           for product in comparison["products"].values()):
        raise ValueError("product comparison has incomplete coverage or topology")
    checks = {check["name"]: check for check in comparison["tolerance_evaluation"]["checks"]}
    expected = {f"{suffix}.diff_rms_over_right_rms" for suffix in PRODUCTS}
    expected |= {".image.beam_area_relative", ".image.beam_kernel_nrmse"}
    if set(checks) != expected:
        raise ValueError("the nine-check comparison changed")
    if any(check["status"] != "passed" or check["ceiling"] != 0.001
           or not np.isfinite(check["actual"]) or check["actual"] > 0.001
           for name, check in checks.items()
           if name not in DIAGNOSTIC_ONLY | REVIEWABLE_COMPARISON):
        raise ValueError("a retained numerical check failed")
    if any(checks[name]["ceiling"] != 0.001 or not np.isfinite(checks[name]["actual"])
           for name in REVIEWABLE_COMPARISON):
        raise ValueError("restored-image review metric is invalid")
    if comparison["status"] not in {"completed", "out_of_tolerance"}:
        raise ValueError("a structural or metadata comparison failed")
    if comparison["status"] == "out_of_tolerance":
        failures = {name for name, check in checks.items() if check["status"] != "passed"}
        if failures - DIAGNOSTIC_ONLY - REVIEWABLE_COMPARISON:
            raise ValueError("out-of-tolerance status includes a hard check")
    return ({name: checks[name]["actual"] for name in sorted(DIAGNOSTIC_ONLY)},
            sorted(name for name in REVIEWABLE_COMPARISON
                   if checks[name]["status"] != "passed" or checks[name]["actual"] > 0.001))


def assess_arrays(native, casa, *, threshold_jy=THRESHOLD_JY):
    if set(native) != {"image", "residual"} or set(casa) != set(native):
        raise ValueError("missing nonlinear CLEAN products")
    shape = casa["image"].shape
    if len(shape) != 2 or shape[0] != shape[1] or any(
        plane.shape != shape or not np.isfinite(plane).all()
        for group in (native, casa) for plane in group.values()
    ):
        raise ValueError("invalid plane shape or nonfinite values")
    pixels = np.arange(shape[0], dtype=np.float64) - shape[0] / 2
    x, y = np.meshgrid(pixels, pixels, indexing="ij")
    clean = np.hypot(x, y) * CELL_ARCSEC <= CLEAN_RADIUS_ARCSEC
    casa_sigma = rms(casa["residual"][clean])
    if casa_sigma <= 0:
        raise ValueError("CASA residual scale is zero")
    residual_delta = native["residual"][clean] - casa["residual"][clean]
    casa_restored_model = casa["image"] - casa["residual"]
    native_restored_model = native["image"] - native["residual"]
    model_scale = rms(casa_restored_model[clean])
    if model_scale <= 0:
        raise ValueError("CASA restored-model scale is zero")
    flux_casa = float(np.sum(casa["image"][clean], dtype=np.float64))
    flux_native = float(np.sum(native["image"][clean], dtype=np.float64))
    if flux_casa == 0:
        raise ValueError("CASA source flux is zero")
    centroids = []
    for group in (native, casa):
        positive = np.maximum(group["image"], 0) * clean
        weight = np.sum(positive, dtype=np.float64)
        if weight <= 0:
            raise ValueError("no positive source flux")
        centroids.append(np.array([
            np.sum(positive * x, dtype=np.float64) / weight,
            np.sum(positive * y, dtype=np.float64) / weight,
        ]))
    measurements = {
        "casa_residual_rms_jy_beam": casa_sigma,
        "native_residual_rms_over_casa": rms(native["residual"][clean]) / casa_sigma,
        "residual_difference_rms_over_casa": rms(residual_delta) / casa_sigma,
        "residual_difference_max_over_casa_rms": float(np.max(np.abs(residual_delta))) / casa_sigma,
        "casa_peak_residual_over_threshold": float(np.max(np.abs(casa["residual"][clean]))) / threshold_jy,
        "native_peak_residual_over_threshold": float(np.max(np.abs(native["residual"][clean]))) / threshold_jy,
        "restored_component_difference_rms_over_casa": rms(
            (native_restored_model - casa_restored_model)[clean]) / model_scale,
        "restored_image_signed_flux_relative": abs(flux_native - flux_casa) / abs(flux_casa),
        "restored_image_positive_centroid_separation_pixels": float(
            np.linalg.norm(centroids[0] - centroids[1])),
    }
    if not all(np.isfinite(value) for value in measurements.values()):
        raise ValueError("nonfinite nonlinear CLEAN measurement")
    limits = {
        "native_residual_rms_over_casa": 1.05,
        "residual_difference_rms_over_casa": 0.05,
        "residual_difference_max_over_casa_rms": 0.5,
        "casa_peak_residual_over_threshold": 1.01,
        "native_peak_residual_over_threshold": 1.01,
        "restored_component_difference_rms_over_casa": 0.001,
        "restored_image_signed_flux_relative": 0.01,
        "restored_image_positive_centroid_separation_pixels": 1.0,
    }
    failed = [key for key, limit in limits.items()
              if key not in REVIEWABLE_SCIENCE and measurements[key] > limit]
    review_triggers = [key for key, limit in limits.items()
                       if key in REVIEWABLE_SCIENCE and measurements[key] > limit]
    return {"measurements": measurements, "limits": limits,
            "status": "failed" if failed else ("review_required" if review_triggers else "passed"),
            "failed": failed, "review_triggers": review_triggers}


def image_shape(path):
    from casatools import image

    handle = image()
    handle.open(str(path))
    try:
        shape = tuple(handle.shape())
        if len(shape) != 4 or shape[2] != 1 or shape[0] != shape[1]:
            raise ValueError(f"expected square singleton-Stokes cube: {path}: {shape}")
        return shape
    finally:
        handle.done()


def read_plane(path, spectral_plane):
    from casatools import image

    handle = image()
    handle.open(str(path))
    try:
        shape = tuple(handle.shape())
        if len(shape) != 4 or shape[2] != 1 or not 0 <= spectral_plane < shape[3]:
            raise ValueError(f"invalid spectral plane {spectral_plane}: {path}: {shape}")
        values = handle.getchunk(blc=[0, 0, 0, spectral_plane],
                                 trc=[shape[0] - 1, shape[1] - 1, 0, spectral_plane])
        if values.ndim != 4 or values.shape[2:] != (1, 1):
            raise ValueError(f"expected singleton spectral/Stokes plane: {path}")
        return np.asarray(values[:, :, 0, 0], dtype=np.float64)
    finally:
        handle.done()


def read_masked_plane(path, spectral_plane):
    from casatools import image

    handle = image()
    handle.open(str(path))
    try:
        shape = tuple(handle.shape())
        if len(shape) != 4 or shape[2] != 1 or not 0 <= spectral_plane < shape[3]:
            raise ValueError(f"invalid spectral plane {spectral_plane}: {path}: {shape}")
        blc = [0, 0, 0, spectral_plane]
        trc = [shape[0] - 1, shape[1] - 1, 0, spectral_plane]
        values = handle.getchunk(blc=blc, trc=trc)
        mask = handle.getchunk(blc=blc, trc=trc, getmask=True)
        if values.ndim != 4 or values.shape[2:] != (1, 1) or mask.shape != values.shape:
            raise ValueError(f"expected singleton spectral/Stokes plane: {path}")
        return (np.asarray(values[:, :, 0, 0], dtype=np.float64),
                np.asarray(mask[:, :, 0, 0], dtype=bool))
    finally:
        handle.done()


def read_restoring_beam(prefix, spectral_plane):
    from casatools import image

    handle = image()
    handle.open(prefix + ".image")
    try:
        return handle.restoringbeam(channel=spectral_plane, polarization=0)
    finally:
        handle.done()


def assess_strict_plane_products(products, beams):
    """Apply retained deterministic-product/beam checks to one spectral plane."""
    measurements = {}
    failed = []
    review_triggers = []
    if set(products) != set(STRICT_PLANE_PRODUCTS):
        raise ValueError("strict per-plane product inventory is incomplete")
    for suffix, sides in products.items():
        left, left_mask = sides["native"]
        right, right_mask = sides["casa"]
        if left.shape != right.shape or left_mask.shape != left.shape or right_mask.shape != right.shape:
            raise ValueError(f"per-plane product shape mismatch: {suffix}")
        if not np.array_equal(left_mask, right_mask):
            failed.append(f"{suffix}.pixel_mask_topology")
        if not np.array_equal(np.isfinite(left), np.isfinite(right)):
            failed.append(f"{suffix}.finite_topology")
        if not np.isfinite(left).all() or not np.isfinite(right).all():
            failed.append(f"{suffix}.nonfinite_values")
        valid = left_mask & right_mask & np.isfinite(left) & np.isfinite(right)
        if not np.any(valid):
            failed.append(f"{suffix}.no_finite_overlap")
            continue
        reference = rms(right[valid])
        difference = rms((left - right)[valid])
        ratio = difference / reference if reference else (0.0 if difference == 0 else float("inf"))
        name = f"{suffix}.diff_rms_over_right_rms"
        measurements[name] = ratio
        if not np.isfinite(ratio):
            failed.append(name)
        elif ratio > 0.001:
            (review_triggers if name in REVIEWABLE_COMPARISON else failed).append(name)
    beam_metrics = scientific_beam_metrics({
        "left": {"restoring_beam": beams["native"]},
        "right": {"restoring_beam": beams["casa"]},
    })
    for metric, value in beam_metrics.items():
        name = ".image." + metric
        measurements[name] = value
        if value is None or not np.isfinite(value) or value > 0.001:
            failed.append(name)
    return {"measurements": measurements, "failed": failed,
            "review_triggers": review_triggers,
            "status": "failed" if failed else ("review_required" if review_triggers else "passed")}


def hash_product_plane(digest, channel, suffix, label, values, mask):
    """Bind a review to values already read for the science assessment."""
    for kind, array, dtype in (("values", values, "<f4"), ("mask", mask, "?")):
        canonical = np.ascontiguousarray(array, dtype=dtype)
        header = json.dumps([channel, suffix, label, kind, canonical.shape],
                            separators=(",", ":")).encode()
        digest.update(len(header).to_bytes(4, "little"))
        digest.update(header)
        digest.update(memoryview(canonical).cast("B"))


def assess_saved_comparison(path, first_channel):
    records = []
    comparison_bytes = path.read_bytes()
    comparison = json.loads(comparison_bytes)
    raw_diagnostics, global_review_triggers = strict_comparison_checks(comparison)
    prefixes = {"native": comparison["left_prefix"], "casa": comparison["right_prefix"]}
    shapes = {label: image_shape(prefix + ".image") for label, prefix in prefixes.items()}
    if shapes["native"] != shapes["casa"]:
        raise ValueError("native/CASA image cube shapes differ")
    count = shapes["native"][3]
    if first_channel < 0 or first_channel + count > 512:
        raise ValueError("channel range exceeds C-array cube")
    product_digest = hashlib.sha256()
    product_digest.update(RULE.encode())
    for plane in range(count):
        channel = first_channel + plane
        products = {
            suffix: {label: read_masked_plane(prefix + suffix, plane)
                     for label, prefix in prefixes.items()}
            for suffix in sorted(ASSESSMENT_PRODUCTS)
        }
        for suffix, sides in products.items():
            for label, (values, mask) in sides.items():
                hash_product_plane(product_digest, channel, suffix, label, values, mask)
        beams = {label: read_restoring_beam(prefix, plane)
                 for label, prefix in prefixes.items()}
        product_digest.update(json.dumps([channel, beams], sort_keys=True,
                                         separators=(",", ":")).encode())
        strict = assess_strict_plane_products(
            {suffix: products[suffix] for suffix in STRICT_PLANE_PRODUCTS}, beams)
        arrays = {label: {suffix: products["." + suffix][label][0]
                          for suffix in ("image", "residual")}
                  for label in prefixes}
        assessment = assess_arrays(arrays["native"], arrays["casa"])
        failed = strict["failed"] + assessment["failed"]
        review_triggers = strict["review_triggers"] + assessment["review_triggers"]
        records.append({"channel": channel, "display_plane": plane,
                        "comparison": str(path), "prefixes": prefixes,
                        "raw_full_cube_component_and_residual_diagnostics": raw_diagnostics,
                        "strict_per_plane": strict,
                        **assessment,
                        "status": "failed" if failed else (
                            "review_required" if review_triggers else "passed"),
                        "failed": failed, "review_triggers": sorted(review_triggers)})
    if global_review_triggers and not any(
        set(global_review_triggers) <= set(row["review_triggers"]) for row in records
    ):
        raise ValueError("full-cube restored-image alert has no failing plane")
    identity = {
        "comparison_sha256": hashlib.sha256(comparison_bytes).hexdigest(),
        "products_sha256": product_digest.hexdigest(),
        "policy_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "global_review_triggers": global_review_triggers,
    }
    return records, identity


def apply_review_manifest(comparisons, review_manifest):
    """Accept only exact, owner-reviewed numerical findings for frozen results."""
    if review_manifest is None:
        return
    if review_manifest.get("rule") != RULE or set(review_manifest) != {"rule", "comparisons"}:
        raise ValueError("review record has the wrong rule or schema")
    provided = review_manifest["comparisons"]
    if len(provided) != len(comparisons):
        raise ValueError("review record comparison count differs")
    for (records, identity), reviewed in zip(comparisons, provided, strict=True):
        if set(reviewed) != {"identity", "reviews"} or reviewed["identity"] != identity:
            raise ValueError("review record is stale for this comparison or policy")
        indexed = {}
        for group in reviewed["reviews"]:
            if set(group) != {"channels", "triggers", "reviewer", "owner_decision",
                              "owner_record", "rationale", "evidence", "visual_review"}:
                raise ValueError("review group is missing required evidence")
            if (group["owner_decision"] != "accept_numerical_sensitivity"
                    or not all(isinstance(group[key], str) and group[key].strip()
                               for key in ("reviewer", "owner_record", "rationale",
                                           "visual_review"))
                    or not isinstance(group["evidence"], list)
                    or not group["evidence"]
                    or not all(isinstance(item, str) and item.strip()
                               for item in group["evidence"])):
                raise ValueError("review group lacks a reason, visual evidence or authority")
            if not isinstance(group["channels"], list) or not group["channels"]:
                raise ValueError("review group has no channels")
            for channel in group["channels"]:
                if channel in indexed:
                    raise ValueError("duplicate channel review")
                indexed[channel] = group
        needed = {row["channel"] for row in records if row["review_triggers"]}
        if set(indexed) != needed:
            raise ValueError("reviewed channels do not match current findings")
        for row in records:
            if row["channel"] not in indexed:
                continue
            if row["failed"] or sorted(indexed[row["channel"]]["triggers"]) != row["review_triggers"]:
                raise ValueError("review cannot override a hard failure or different finding")
            row["status"] = "accepted_reviewed"
            row["review"] = indexed[row["channel"]]


def finalize_saved_report(source_bytes, manifest):
    """Apply an exact review to measured evidence without reopening any image."""
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    if (set(manifest) != {"rule", "comparisons", "assessment_sha256"}
            or manifest["assessment_sha256"] != source_sha256):
        raise ValueError("review does not identify the exact saved assessment")
    report = json.loads(source_bytes)
    if (set(report) != {"rule", "channels", "comparisons", "status"}
            or report["rule"] != RULE or report["status"] != "review_required"
            or not report["channels"] or not report["comparisons"]):
        raise ValueError("source is not an unreviewed assessment for this rule")
    grouped = []
    seen_paths = set()
    seen_channels = set()
    for row in report["channels"]:
        if row["failed"] or row["status"] != (
            "review_required" if row["review_triggers"] else "passed"
        ) or row["channel"] in seen_channels:
            raise ValueError("source assessment has a hard failure or invalid channel")
        seen_channels.add(row["channel"])
        path = row["comparison"]
        if not grouped or grouped[-1][0] != path:
            if path in seen_paths:
                raise ValueError("source comparisons are interleaved")
            seen_paths.add(path)
            grouped.append((path, []))
        grouped[-1][1].append(row)
    if len(grouped) != len(report["comparisons"]):
        raise ValueError("source comparison inventory differs")
    comparisons = [(rows, identity) for (_, rows), identity in
                   zip(grouped, report["comparisons"], strict=True)]
    apply_review_manifest(comparisons, {"rule": manifest["rule"],
                                        "comparisons": manifest["comparisons"]})
    if any(row["status"] == "review_required" for row in report["channels"]):
        raise ValueError("review left a numerical alert unresolved")
    report["status"] = "accepted_reviewed"
    report["source_assessment_sha256"] = source_sha256
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=Path)
    parser.add_argument("--comparison", type=Path,
                        help="full-array comparator result for one contiguous cube")
    parser.add_argument("--first-channel", type=int,
                        help="first output channel of --comparison; all planes are assessed")
    parser.add_argument("--finalize-report", type=Path,
                        help="apply a review to an existing assessment without rereading products")
    parser.add_argument("--review-manifest", type=Path,
                        help="exact assessment-bound owner review; requires --finalize-report")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("assessment output must be new; retained evidence is immutable")
    if args.finalize_report is not None:
        if (args.records is not None or args.comparison is not None
                or args.first_channel is not None or args.review_manifest is None):
            raise ValueError("review finalization requires only a saved report and manifest")
        manifest = json.loads(args.review_manifest.read_text())
        report = finalize_saved_report(args.finalize_report.read_bytes(), manifest)
    elif args.comparison is not None:
        if args.records is not None or args.first_channel is None:
            raise ValueError("--comparison requires --first-channel and no --records")
        comparisons = [(args.first_channel, args.comparison)]
    else:
        if args.records is None or args.first_channel is not None:
            raise ValueError("legacy three-plane assessment requires --records")
        comparisons = [(channel, args.records / f"turnaround-native-casa-ch{channel:03}"
                        / "result.json") for channel in CHANNELS]
    if args.finalize_report is None:
        if args.review_manifest is not None:
            raise ValueError("review requires --finalize-report; no second product pass")
        results = [assess_saved_comparison(path, first_channel)
                   for first_channel, path in comparisons]
        records = [row for rows, _ in results for row in rows]
        statuses = {row["status"] for row in records}
        status = ("failed" if "failed" in statuses else
                  "review_required" if "review_required" in statuses else "passed")
        report = {"rule": RULE, "channels": records,
                  "comparisons": [identity for _, identity in results],
                  "status": status}
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": report["status"], "channels": [
        {"channel": row["channel"], "status": row["status"],
         "failed": row["failed"], "review_triggers": row["review_triggers"]}
        for row in report["channels"]]}, indent=2))
    if report["status"] not in ("passed", "accepted_reviewed"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
