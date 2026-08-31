#!/usr/bin/env python3
"""Compare the focused T43 MT-MFS CLEAN trajectory with its frozen CASA oracle.

The casa-rs artifact deliberately records solver-level Taylor state rather than
published image products.  Product publication belongs to T44/#530.  CASA
component order is not compared: this gate checks the four cycle summaries,
the final Taylor model and residual planes, and the spectral coefficient ratio
on reference-significant model support.

Run this script with a Python environment that provides ``casatools``.  Reading
the frozen CASA images does not execute ``tclean`` or otherwise regenerate the
oracle.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np


SUMMARY_SCHEMA = "casa-rs-t43-mtmfs-clean-comparison-v1"
RUST_SCHEMA = "casa-rs-t43-mtmfs-clean-v1"
NRMS_CEILING = 1.0e-3
SCALE_FLOOR_RATIO = 1.0e-7
SPECTRAL_SUPPORT_RATIO = 1.0e-3
EXPECTED_CYCLES = 4
EXPECTED_ITERATIONS_PER_CYCLE = 2
PRODUCT_SUFFIXES = {
    "model_tt0": ".model.tt0",
    "model_tt1": ".model.tt1",
    "residual_tt0": ".residual.tt0",
    "residual_tt1": ".residual.tt1",
}


class ComparisonError(RuntimeError):
    """Evidence is missing or malformed and cannot be compared safely."""


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ComparisonError(f"{label} must be an object")
    return value


def _vector(value: Any, length: int, label: str) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if result.shape != (length,):
        raise ComparisonError(f"{label} has shape {result.shape}; expected {(length,)}")
    return np.ascontiguousarray(result)


def _plane(value: Any, shape: tuple[int, int], label: str) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if result.size != shape[0] * shape[1]:
        raise ComparisonError(
            f"{label} has {result.size} values; expected {shape[0] * shape[1]}"
        )
    return np.ascontiguousarray(result.reshape(shape))


def _normalized_rms(
    candidate: np.ndarray, reference: np.ndarray, support: np.ndarray | None = None
) -> dict[str, Any]:
    if candidate.shape != reference.shape:
        raise ComparisonError(
            f"NRMS shapes differ: {candidate.shape} != {reference.shape}"
        )
    selected = np.ones(reference.shape, dtype=bool) if support is None else support
    if selected.shape != reference.shape or not np.any(selected):
        raise ComparisonError("NRMS support is empty or has the wrong shape")
    actual = np.asarray(candidate[selected], dtype=np.float64)
    expected = np.asarray(reference[selected], dtype=np.float64)
    finite = bool(np.all(np.isfinite(actual)) and np.all(np.isfinite(expected)))
    if not finite:
        return {
            "pass": False,
            "finite": False,
            "support_count": int(np.count_nonzero(selected)),
            "ceiling": NRMS_CEILING,
        }
    difference_rms = float(np.sqrt(np.mean(np.square(actual - expected))))
    reference_rms = float(np.sqrt(np.mean(np.square(expected))))
    reference_scale = float(np.max(np.abs(expected)))
    denominator = max(reference_rms, reference_scale * SCALE_FLOOR_RATIO)
    value = 0.0 if denominator == 0.0 and difference_rms == 0.0 else (
        None if denominator == 0.0 else difference_rms / denominator
    )
    return {
        "pass": value is not None and value <= NRMS_CEILING,
        "finite": True,
        "support_count": int(np.count_nonzero(selected)),
        "difference_rms": difference_rms,
        "reference_rms": reference_rms,
        "reference_scale": reference_scale,
        "denominator": denominator,
        "normalized_rms": value,
        "ceiling": NRMS_CEILING,
    }


def _load_casa_product(path: Path) -> tuple[np.ndarray, np.ndarray]:
    try:
        from casatools import image as image_tool
    except ImportError as error:
        raise ComparisonError(
            "reading the frozen CASA images requires Python with casatools"
        ) from error

    tool = image_tool()
    try:
        if not tool.open(str(path)):
            raise ComparisonError(f"CASA image could not be opened: {path}")
        values = np.asarray(tool.getchunk(), dtype=np.float64)
        mask = np.asarray(tool.getchunk(getmask=True), dtype=bool)
    finally:
        tool.close()
    values = np.squeeze(values)
    mask = np.squeeze(mask)
    if values.ndim != 2 or mask.shape != values.shape:
        raise ComparisonError(
            f"CASA image {path} has values {values.shape} and mask {mask.shape}; "
            "expected one two-dimensional plane"
        )
    return np.ascontiguousarray(values), np.ascontiguousarray(mask)


def load_casa_products(prefix: Path) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    products: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for name, suffix in PRODUCT_SUFFIXES.items():
        path = Path(f"{prefix}{suffix}")
        if not path.is_dir():
            raise ComparisonError(f"frozen CASA product is missing: {path}")
        products[name] = _load_casa_product(path)
    shapes = {values.shape for values, _ in products.values()}
    if len(shapes) != 1:
        raise ComparisonError(f"frozen CASA Taylor products disagree on shape: {shapes}")
    return products


def _casa_cycles(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    summary = _object(result.get("summaryminor"), "CASA summaryminor")
    try:
        leaf = _object(
            _object(_object(summary["0"], "CASA summaryminor[0]")["0"],
                    "CASA summaryminor[0][0]")["0"],
            "CASA summaryminor[0][0][0]",
        )
    except KeyError as error:
        raise ComparisonError("CASA summaryminor lacks the expected single-field leaf") from error
    keys = ("iterDone", "cycleThresh", "peakRes", "modelFlux")
    arrays = {key: _vector(leaf.get(key), EXPECTED_CYCLES, f"CASA {key}") for key in keys}
    return [
        {
            "iterations": int(arrays["iterDone"][index]),
            "cycle_threshold": float(arrays["cycleThresh"][index]),
            "peak_residual": float(arrays["peakRes"][index]),
            "model_flux": float(arrays["modelFlux"][index]),
        }
        for index in range(EXPECTED_CYCLES)
    ]


def compare_documents(
    casa_result: Mapping[str, Any],
    casa_products: Mapping[str, tuple[np.ndarray, np.ndarray]],
    rust: Mapping[str, Any],
) -> dict[str, Any]:
    if rust.get("schema") != RUST_SCHEMA:
        raise ComparisonError(f"casa-rs artifact must use schema {RUST_SCHEMA!r}")
    geometry = _object(rust.get("geometry"), "casa-rs geometry")
    raw_shape = geometry.get("shape")
    if not isinstance(raw_shape, list) or len(raw_shape) != 2:
        raise ComparisonError("casa-rs geometry.shape must contain two dimensions")
    shape = (int(raw_shape[0]), int(raw_shape[1]))
    if geometry.get("layout") != "x,y":
        raise ComparisonError("casa-rs geometry.layout must be 'x,y'")
    casa_shape = next(iter(casa_products.values()))[0].shape
    if shape != casa_shape:
        raise ComparisonError(f"casa-rs shape {shape} differs from CASA {casa_shape}")

    failures: list[str] = []

    def record(label: str, evidence: dict[str, Any]) -> dict[str, Any]:
        evidence["pass"] = bool(evidence["pass"])
        if not evidence["pass"]:
            failures.append(label)
        return evidence

    casa_cycles = _casa_cycles(casa_result)
    trajectory = _object(rust.get("trajectory"), "casa-rs trajectory")
    rust_cycles_raw = trajectory.get("cycles")
    if not isinstance(rust_cycles_raw, list) or len(rust_cycles_raw) != EXPECTED_CYCLES:
        raise ComparisonError(f"casa-rs trajectory must contain {EXPECTED_CYCLES} cycles")
    rust_cycles = [
        _object(cycle, f"casa-rs trajectory cycle {index}")
        for index, cycle in enumerate(rust_cycles_raw)
    ]
    iteration_counts = [int(cycle.get("iterations", -1)) for cycle in rust_cycles]
    iteration_evidence = record(
        "trajectory_iteration_envelope",
        {
            "pass": iteration_counts
            == [EXPECTED_ITERATIONS_PER_CYCLE] * EXPECTED_CYCLES
            and int(trajectory.get("total_iterations", -1))
            == int(casa_result.get("iterdone", -2))
            == EXPECTED_CYCLES * EXPECTED_ITERATIONS_PER_CYCLE,
            "casa": [cycle["iterations"] for cycle in casa_cycles],
            "casa_rs": iteration_counts,
            "casa_total": int(casa_result.get("iterdone", -1)),
            "casa_rs_total": int(trajectory.get("total_iterations", -1)),
        },
    )
    stopping_evidence = record(
        "trajectory_stopping_envelope",
        {
            "pass": all(cycle.get("stop_reason") == "iteration_bound" for cycle in rust_cycles)
            and trajectory.get("stop_reason") == "iteration_limit"
            and casa_result.get("stopcode") == 1,
            "casa_stopcode": casa_result.get("stopcode"),
            "casa_description": casa_result.get("stopDescription"),
            "casa_rs_cycle_reasons": [cycle.get("stop_reason") for cycle in rust_cycles],
            "casa_rs_reason": trajectory.get("stop_reason"),
        },
    )
    trajectory_metrics: dict[str, Any] = {}
    for field in ("cycle_threshold", "peak_residual", "model_flux"):
        candidate = _vector(
            [cycle.get(field) for cycle in rust_cycles], EXPECTED_CYCLES, f"casa-rs {field}"
        )
        reference = _vector(
            [cycle[field] for cycle in casa_cycles], EXPECTED_CYCLES, f"CASA {field}"
        )
        trajectory_metrics[field] = record(
            f"trajectory_{field}_normalized_rms", _normalized_rms(candidate, reference)
        )

    rust_products_raw = _object(rust.get("products"), "casa-rs products")
    rust_products = {
        name: _plane(rust_products_raw.get(name), shape, f"casa-rs products.{name}")
        for name in PRODUCT_SUFFIXES
    }
    product_metrics: dict[str, Any] = {}
    for name, candidate in rust_products.items():
        reference, casa_mask = casa_products[name]
        support = casa_mask & np.isfinite(reference)
        product_metrics[name] = record(
            f"final_{name}_normalized_rms",
            _normalized_rms(candidate, reference, support),
        )

    casa_tt0, casa_tt0_mask = casa_products["model_tt0"]
    casa_tt1, casa_tt1_mask = casa_products["model_tt1"]
    reference_peak = float(np.max(np.abs(casa_tt0[casa_tt0_mask])))
    spectral_support = (
        casa_tt0_mask
        & casa_tt1_mask
        & np.isfinite(casa_tt0)
        & np.isfinite(casa_tt1)
        & (np.abs(casa_tt0) >= reference_peak * SPECTRAL_SUPPORT_RATIO)
    )
    if not np.any(spectral_support):
        raise ComparisonError("CASA model has no reference-significant spectral support")
    rust_tt0 = rust_products["model_tt0"]
    rust_tt1 = rust_products["model_tt1"]
    with np.errstate(divide="ignore", invalid="ignore"):
        casa_spectrum = casa_tt1 / casa_tt0
        rust_spectrum = rust_tt1 / rust_tt0
    spectral_metric = record(
        "recovered_spectral_behavior_normalized_rms",
        _normalized_rms(rust_spectrum, casa_spectrum, spectral_support),
    )
    spectral_metric["definition"] = "model.tt1/model.tt0"
    spectral_metric["support_floor_fraction_of_casa_tt0_peak"] = SPECTRAL_SUPPORT_RATIO

    return {
        "schema": SUMMARY_SCHEMA,
        "pass": not failures,
        "failures": failures,
        "contract": {
            "component_order_normative": False,
            "normalized_rms_ceiling": NRMS_CEILING,
            "cycle_count": EXPECTED_CYCLES,
            "iterations_per_cycle": EXPECTED_ITERATIONS_PER_CYCLE,
        },
        "trajectory": {
            "iterations": iteration_evidence,
            "stopping": stopping_evidence,
            "metrics": trajectory_metrics,
        },
        "products": product_metrics,
        "spectral_behavior": spectral_metric,
    }


def compare(casa_prefix: Path, casa_result_path: Path, rust_path: Path) -> dict[str, Any]:
    if not casa_result_path.is_file():
        raise ComparisonError(f"CASA result JSON is missing: {casa_result_path}")
    if not rust_path.is_file():
        raise ComparisonError(f"casa-rs artifact is missing: {rust_path}")
    casa_result = _object(
        json.loads(casa_result_path.read_text(encoding="utf-8")), "CASA result"
    )
    rust = _object(json.loads(rust_path.read_text(encoding="utf-8")), "casa-rs artifact")
    return compare_documents(casa_result, load_casa_products(casa_prefix), rust)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa-prefix", required=True, type=Path)
    parser.add_argument("--casa-result", required=True, type=Path)
    parser.add_argument("--rust-json", required=True, type=Path)
    parser.add_argument("--summary-output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        summary = compare(args.casa_prefix, args.casa_result, args.rust_json)
    except (ComparisonError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"t43_mtmfs_clean_compare: {error}", file=sys.stderr)
        return 2
    encoded = json.dumps(summary, indent=2, sort_keys=True, allow_nan=False)
    print(encoded)
    if args.summary_output is not None:
        args.summary_output.parent.mkdir(parents=True, exist_ok=True)
        args.summary_output.write_text(encoded + "\n", encoding="utf-8")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
