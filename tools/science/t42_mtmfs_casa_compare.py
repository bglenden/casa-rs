#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the focused T42 CASA and casa-rs MT-MFS normal operators.

The script accepts the frozen CASA two-SPW NPZ and the deterministic casa-rs
JSON artifact, prints one machine-readable summary, and exits nonzero unless
every structural and scientific gate passes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np


SUMMARY_SCHEMA = "casa-rs-t42-mtmfs-casa-comparison-v1"
RUST_SCHEMA = "casa-rs-t42-mtmfs-normal-v1"
CEILING = 1.0e-3
SCALE_FLOOR_RATIO = 1.0e-7
HANKEL_MOMENTS = np.asarray([[0, 1], [1, 2]], dtype=np.int64)


class ComparisonError(RuntimeError):
    """Malformed evidence that cannot be compared safely."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def array(
    value: Any,
    dtype: np.dtype[Any] | type[Any],
    shape: tuple[int, ...],
    label: str,
) -> np.ndarray:
    result = np.asarray(value, dtype=dtype)
    expected = int(np.prod(shape, dtype=np.int64))
    if result.size != expected:
        raise ComparisonError(f"{label} has {result.size} values; expected {expected}")
    return np.ascontiguousarray(result.reshape(shape))


def oracle_array(
    archive: np.lib.npyio.NpzFile,
    key: str,
    dtype: np.dtype[Any] | type[Any],
    shape: tuple[int, ...],
) -> np.ndarray:
    if key not in archive.files:
        raise ComparisonError(f"CASA oracle lacks {key!r}")
    result = archive[key]
    if result.dtype != np.dtype(dtype) or result.shape != shape:
        raise ComparisonError(
            f"CASA {key!r} is {result.dtype}{result.shape}; "
            f"expected {np.dtype(dtype)}{shape}"
        )
    return np.asarray(result) if shape == () else np.ascontiguousarray(result)


def f64_bits(value: float) -> int:
    return int(np.asarray(value, dtype=np.float64).view(np.uint64))


def f32_bits(values: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.float32).view(np.uint32)


def exact(actual: np.ndarray, expected: np.ndarray) -> dict[str, Any]:
    if actual.shape != expected.shape:
        raise ComparisonError(f"exact shapes differ: {actual.shape} != {expected.shape}")
    mismatch = np.asarray(actual != expected, dtype=bool)
    locations = np.argwhere(mismatch)
    return {
        "pass": locations.size == 0,
        "value_count": int(actual.size),
        "mismatch_count": int(np.count_nonzero(mismatch)),
        "first_mismatch_index": (
            None if locations.size == 0 else [int(value) for value in locations[0]]
        ),
    }


def record(
    failures: list[str], label: str, result: dict[str, Any]
) -> dict[str, Any]:
    result["pass"] = bool(result["pass"])
    if not result["pass"]:
        failures.append(label)
    return result


def normalized_rms(
    candidate: np.ndarray,
    reference: np.ndarray,
    support: np.ndarray,
    reference_scale: float,
) -> dict[str, Any]:
    candidate = np.asarray(candidate[support], dtype=np.float64)
    reference = np.asarray(reference[support], dtype=np.float64)
    finite = bool(np.all(np.isfinite(candidate)) and np.all(np.isfinite(reference)))
    if not finite:
        return {"pass": False, "finite": False, "shared_support_count": int(support.sum())}
    difference_rms = float(np.sqrt(np.mean(np.square(candidate - reference))))
    reference_rms = float(np.sqrt(np.mean(np.square(reference))))
    floor = reference_scale * SCALE_FLOOR_RATIO
    denominator = max(reference_rms, floor)
    value = difference_rms / denominator
    return {
        "pass": value <= CEILING,
        "finite": True,
        "shared_support_count": int(support.sum()),
        "difference_rms": difference_rms,
        "reference_rms": reference_rms,
        "reference_scale": reference_scale,
        "scale_floor": floor,
        "denominator": denominator,
        "normalized_rms": value,
        "ceiling": CEILING,
    }


def spectral_normalized_rms(
    candidate: np.ndarray, reference: np.ndarray
) -> dict[str, Any]:
    candidate = np.asarray(candidate, dtype=np.float64)
    reference = np.asarray(reference, dtype=np.float64)
    if candidate.shape != reference.shape:
        raise ComparisonError(
            f"spectral shapes differ: {candidate.shape} != {reference.shape}"
        )
    finite = bool(np.all(np.isfinite(candidate)) and np.all(np.isfinite(reference)))
    if not finite:
        return {"pass": False, "finite": False, "value_count": int(reference.size)}
    difference = candidate - reference
    difference_rms = float(np.sqrt(np.mean(np.square(difference))))
    reference_rms = float(np.sqrt(np.mean(np.square(reference))))
    reference_scale = float(np.max(np.abs(reference)))
    denominator = max(reference_rms, reference_scale * SCALE_FLOOR_RATIO)
    value = difference_rms / denominator
    relative = np.divide(
        np.abs(difference),
        np.maximum(np.abs(reference), reference_scale * SCALE_FLOOR_RATIO),
    )
    return {
        "pass": value <= CEILING,
        "finite": True,
        "value_count": int(reference.size),
        "difference_rms": difference_rms,
        "signed_bias": float(np.mean(difference)),
        "maximum_absolute_error": float(np.max(np.abs(difference))),
        "maximum_relative_error": float(np.max(relative)),
        "reference_rms": reference_rms,
        "reference_scale": reference_scale,
        "denominator": denominator,
        "normalized_rms": value,
        "ceiling": CEILING,
    }


def relative_weight(candidate: float, reference: float, w0: float) -> dict[str, Any]:
    finite = bool(np.isfinite(candidate) and np.isfinite(reference) and np.isfinite(w0))
    if not finite:
        return {"pass": False, "finite": False}
    denominator = max(abs(reference), abs(w0) * SCALE_FLOOR_RATIO)
    error = abs(candidate - reference)
    value = error / denominator
    return {
        "pass": value <= CEILING,
        "finite": True,
        "candidate": candidate,
        "reference": reference,
        "absolute_error": error,
        "denominator": denominator,
        "relative_error": value,
        "ceiling": CEILING,
    }


def section(document: dict[str, Any], name: str) -> dict[str, Any]:
    value = document.get(name)
    if not isinstance(value, dict):
        raise ComparisonError(f"casa-rs artifact lacks object {name!r}")
    return value


def compare(casa_path: Path, rust_path: Path) -> dict[str, Any]:
    if not casa_path.is_file() or not rust_path.is_file():
        missing = casa_path if not casa_path.is_file() else rust_path
        raise ComparisonError(f"input does not exist: {missing}")
    rust = json.loads(rust_path.read_text(encoding="utf-8"))
    if not isinstance(rust, dict) or rust.get("schema") != RUST_SCHEMA:
        raise ComparisonError(f"casa-rs artifact must use schema {RUST_SCHEMA!r}")
    source = section(rust, "source")
    geometry = section(rust, "geometry")
    spectral = section(rust, "spectral")
    normal = section(rust, "normal")
    if geometry.get("shape") != [128, 128] or geometry.get("layout") != "term,x,y":
        raise ComparisonError("casa-rs geometry is not [128,128] term,x,y")

    spatial = (128, 128)
    dirty_shape = (2, *spatial)
    psf_shape = (3, *spatial)
    frequency_shape = (24, 16)
    basis_shape = (2, *frequency_shape)
    with np.load(casa_path, allow_pickle=False) as casa:
        casa_rows = oracle_array(casa, "ms_row_id", np.int64, (24,))
        casa_frequency = oracle_array(
            casa, "frequency_lsrk_by_row_hz_f64", np.float64, frequency_shape
        )
        casa_basis = oracle_array(
            casa, "taylor_basis_f32_bits", np.uint32, basis_shape
        )
        casa_reference = float(
            oracle_array(casa, "reference_frequency_hz_f64", np.float64, ())
        )
        casa_reference_bits = int(
            oracle_array(casa, "reference_frequency_f64_bits", np.uint64, ())
        )
        casa_dirty = oracle_array(
            casa, "dirty_taylor_normalized_f32", np.float32, dirty_shape
        )
        casa_psf = oracle_array(
            casa, "psf_moments_normalized_f32", np.float32, psf_shape
        )
        casa_dirty_masks = oracle_array(casa, "dirty_masks_bool", np.bool_, dirty_shape)
        casa_psf_masks = oracle_array(casa, "psf_masks_bool", np.bool_, psf_shape)
        casa_weights = oracle_array(casa, "sum_weights_f32", np.float32, (3,))
        casa_weight_masks = oracle_array(
            casa, "sum_weight_masks_bool", np.bool_, (3,)
        )
        casa_support = oracle_array(casa, "valid_support_bool", np.bool_, spatial)

    failures: list[str] = []
    selection = exact(
        array(source.get("physical_rows"), np.int64, (24,), "source.physical_rows"),
        casa_rows,
    )
    selection.update(
        selected_samples=int(source.get("selected_samples", -1)),
        payload_passes=int(source.get("payload_passes", -1)),
        maximum_live_source_blocks=int(source.get("maximum_live_source_blocks", -1)),
    )
    selection["pass"] = bool(selection["pass"]) and (
        selection["selected_samples"] == 24 * 16 * 4
        and selection["payload_passes"] == 1
        and 0 < selection["maximum_live_source_blocks"] <= 2
    )
    record(failures, "matched_selection_and_bounded_source", selection)

    rust_frequency = array(
        spectral.get("evaluated_frequency_lsrk_hz_f64"),
        np.float64,
        frequency_shape,
        "spectral.evaluated_frequency_lsrk_hz_f64",
    )
    rust_basis = array(
        spectral.get("taylor_basis_f32_bits"),
        np.uint32,
        basis_shape,
        "spectral.taylor_basis_f32_bits",
    )
    rust_reference = float(geometry.get("reference_frequency_hz_f64", np.nan))
    rust_reference_bits = int(geometry.get("reference_frequency_f64_bits", -1))
    reference = record(
        failures,
        "reference_frequency_bits",
        {
            "pass": len(
                {
                    casa_reference_bits,
                    f64_bits(casa_reference),
                    rust_reference_bits,
                    f64_bits(rust_reference),
                }
            )
            == 1,
            "casa_declared_bits": casa_reference_bits,
            "casa_value_bits": f64_bits(casa_reference),
            "rust_declared_bits": rust_reference_bits,
            "rust_value_bits": f64_bits(rust_reference),
        },
    )
    frequency_bits = exact(
        rust_frequency.view(np.uint64), casa_frequency.view(np.uint64)
    )
    frequency = record(
        failures,
        "frequency_normalized_rms",
        spectral_normalized_rms(rust_frequency, casa_frequency),
    )
    frequency["bitwise_diagnostic"] = frequency_bits
    casa_rounded_frequency = casa_frequency.astype(np.float32).astype(np.float64)
    casa_basis_formula = np.stack(
        (
            np.ones(frequency_shape, dtype=np.float32),
            ((casa_rounded_frequency - casa_reference) / casa_reference).astype(np.float32),
        )
    ).view(np.uint32)
    rust_rounded_frequency = rust_frequency.astype(np.float32).astype(np.float64)
    rust_basis_formula = np.stack(
        (
            np.ones(frequency_shape, dtype=np.float32),
            ((rust_rounded_frequency - rust_reference) / rust_reference).astype(np.float32),
        )
    ).view(np.uint32)
    casa_basis_values = casa_basis.view(np.float32)
    rust_basis_values = rust_basis.view(np.float32)
    basis_checks = {
        "casa_vs_rust": {
            "term0_exact": record(
                failures,
                "taylor_basis_term0_bits",
                exact(rust_basis[0], casa_basis[0]),
            ),
            "term1_normalized_rms": record(
                failures,
                "taylor_basis_term1_normalized_rms",
                spectral_normalized_rms(rust_basis_values[1], casa_basis_values[1]),
            ),
            "bitwise_diagnostic": exact(rust_basis, casa_basis),
        },
        "casa_vs_formula": record(
            failures, "taylor_basis_casa_formula", exact(casa_basis, casa_basis_formula)
        ),
        "rust_vs_formula": record(
            failures, "taylor_basis_rust_formula", exact(rust_basis, rust_basis_formula)
        ),
    }

    rust_dirty_real = array(
        normal.get("dirty_real_f64"), np.float64, dirty_shape, "normal.dirty_real_f64"
    )
    rust_dirty_imag = array(
        normal.get("dirty_imag_f64"), np.float64, dirty_shape, "normal.dirty_imag_f64"
    )
    rust_psf_real = array(
        normal.get("psf_real_f64"), np.float64, psf_shape, "normal.psf_real_f64"
    )
    rust_psf_imag = array(
        normal.get("psf_imag_f64"), np.float64, psf_shape, "normal.psf_imag_f64"
    )
    rust_dirty = array(
        normal.get("dirty_normalized_f32"), np.float32, dirty_shape, "normal.dirty_normalized_f32"
    )
    rust_psf = array(
        normal.get("psf_normalized_f32"), np.float32, psf_shape, "normal.psf_normalized_f32"
    )
    rust_weights_f64 = array(
        normal.get("sum_weights_f64"), np.float64, (3,), "normal.sum_weights_f64"
    )
    rust_weights = array(
        normal.get("sum_weights_f32"), np.float32, (3,), "normal.sum_weights_f32"
    )
    rust_support = array(
        normal.get("valid_support_bool"), np.bool_, spatial, "normal.valid_support_bool"
    )

    derived_casa_support = (
        np.all(casa_dirty_masks, axis=0)
        & np.all(casa_psf_masks, axis=0)
        & np.all(np.isfinite(casa_dirty), axis=0)
        & np.all(np.isfinite(casa_psf), axis=0)
    )
    support_derivation = record(
        failures, "casa_support_derivation", exact(casa_support, derived_casa_support)
    )
    disagreement = casa_support != rust_support
    support_match = record(
        failures,
        "support_match",
        {
            "pass": not np.any(disagreement)
            and normal.get("support_validity") == "valid"
            and np.all(rust_support),
            "casa_valid_count": int(casa_support.sum()),
            "rust_valid_count": int(rust_support.sum()),
            "shared_valid_count": int(np.count_nonzero(casa_support & rust_support)),
            "casa_only_count": int(np.count_nonzero(casa_support & ~rust_support)),
            "rust_only_count": int(np.count_nonzero(rust_support & ~casa_support)),
            "mismatch_count": int(np.count_nonzero(disagreement)),
            "rust_support_validity": normal.get("support_validity"),
        },
    )
    weight_support = record(
        failures,
        "casa_sum_weight_support",
        {"pass": np.all(casa_weight_masks), "valid_count": int(casa_weight_masks.sum())},
    )
    shared_support = casa_support & rust_support
    if not np.any(shared_support):
        raise ComparisonError("CASA and casa-rs have no shared valid support")

    dirty_divisor = float(normal.get("dirty_divisor_f64", np.nan))
    psf_divisor = float(normal.get("psf_divisor_f64", np.nan))
    calculated_psf_divisor = float(np.max(rust_psf_real[0]))
    dirty_divisor_valid = bool(
        dirty_divisor > 0.0 and f64_bits(dirty_divisor) == f64_bits(rust_weights_f64[0])
    )
    psf_divisor_valid = bool(
        psf_divisor > 0.0 and f64_bits(psf_divisor) == f64_bits(calculated_psf_divisor)
    )
    normalization = {
        "catalog": normal.get("catalog"),
        "dirty_divisor_is_w0": record(
            failures, "dirty_normalization_divisor",
            {"pass": dirty_divisor_valid, "divisor": dirty_divisor, "w0": float(rust_weights_f64[0])},
        ),
        "psf_divisor_is_positive_p0_max": record(
            failures, "psf_normalization_divisor",
            {"pass": psf_divisor_valid, "divisor": psf_divisor, "p0_max": calculated_psf_divisor},
        ),
        "sum_weight_f32_rounding": record(
            failures, "sum_weight_f32_rounding",
            exact(f32_bits(rust_weights), f32_bits(rust_weights_f64.astype(np.float32))),
        ),
        "casa_p0_positive_peak": record(
            failures, "casa_p0_normalization",
            {"pass": float(np.max(casa_psf[0])) == 1.0, "peak": float(np.max(casa_psf[0]))},
        ),
    }
    if normal.get("catalog") != "unnormalized_taylor_block_v1":
        failures.append("normal_catalog")
    normalization["dirty_payload_follows_divisor"] = (
        record(
            failures,
            "dirty_normalization_payload",
            exact(f32_bits(rust_dirty), f32_bits((rust_dirty_real / dirty_divisor).astype(np.float32))),
        )
        if dirty_divisor_valid
        else {"pass": False}
    )
    normalization["psf_payload_follows_divisor"] = (
        record(
            failures,
            "psf_normalization_payload",
            exact(f32_bits(rust_psf), f32_bits((rust_psf_real / psf_divisor).astype(np.float32))),
        )
        if psf_divisor_valid
        else {"pass": False}
    )

    hankel = record(
        failures,
        "hankel_normal_block_mapping",
        exact(
            array(normal.get("hankel_moment_indices"), np.int64, (2, 2), "normal.hankel_moment_indices"),
            HANKEL_MOMENTS,
        ),
    )
    hankel["mapping"] = {"H00": "P0", "H01": "P1", "H10": "P1", "H11": "P2"}

    dirty_scale = float(np.max(np.abs(casa_dirty[0][shared_support])))
    psf_scale = float(np.max(casa_psf[0][shared_support]))
    if not (dirty_scale > 0.0 and psf_scale > 0.0):
        raise ComparisonError("CASA D0/P0 reference scale is not positive")
    dirty_metrics = {
        f"D{term}": record(
            failures, f"D{term}_normalized_rms",
            normalized_rms(rust_dirty[term], casa_dirty[term], shared_support, dirty_scale),
        )
        for term in range(2)
    }
    psf_metrics = {
        f"P{moment}": record(
            failures, f"P{moment}_normalized_rms",
            normalized_rms(rust_psf[moment], casa_psf[moment], shared_support, psf_scale),
        )
        for moment in range(3)
    }
    weight_metrics = {
        f"W{moment}": record(
            failures, f"W{moment}_relative_error",
            relative_weight(float(rust_weights[moment]), float(casa_weights[moment]), float(casa_weights[0])),
        )
        for moment in range(3)
    }
    finite = record(
        failures,
        "rust_payload_finite",
        {
            "pass": all(
                np.all(np.isfinite(values))
                for values in (
                    rust_dirty_real, rust_dirty_imag, rust_psf_real, rust_psf_imag,
                    rust_dirty, rust_psf, rust_weights_f64, rust_weights,
                )
            )
        },
    )
    return {
        "schema": SUMMARY_SCHEMA,
        "status": "pass" if not failures else "fail",
        "inputs": {
            "casa_npz": str(casa_path.resolve()),
            "casa_npz_sha256": sha256_file(casa_path),
            "rust_json": str(rust_path.resolve()),
            "rust_json_sha256": sha256_file(rust_path),
        },
        "contract": {
            "normalized_rms_ceiling": CEILING,
            "reference_scale_floor_ratio": SCALE_FLOOR_RATIO,
            "dirty_reference_scale": "positive absolute peak of CASA D0",
            "psf_reference_scale": "positive peak of CASA P0",
            "spectral_comparison": "normalized RMS at the common 0.1% scientific ceiling; each implementation must remain bit-exact to its own declared Taylor formula",
            "sum_weight_denominator": "max(abs(Wk),abs(W0)*1e-7)",
        },
        "selection": selection,
        "spectral": {"reference_frequency": reference, "frequencies": frequency, "basis": basis_checks},
        "support": {
            "casa_declared_vs_derived": support_derivation,
            "casa_vs_rust": support_match,
            "casa_sum_weights": weight_support,
        },
        "normalization": normalization,
        "hankel": hankel,
        "payload_finite": finite,
        "metrics": {"dirty_taylor_terms": dirty_metrics, "psf_moments": psf_metrics, "sum_weights": weight_metrics},
        "failures": failures,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa-npz", required=True, type=Path)
    parser.add_argument("--rust-json", required=True, type=Path)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        result = compare(args.casa_npz, args.rust_json)
    except (ComparisonError, KeyError, OSError, TypeError, ValueError) as error:
        result = {
            "schema": SUMMARY_SCHEMA,
            "status": "fail",
            "inputs": {"casa_npz": str(args.casa_npz), "rust_json": str(args.rust_json)},
            "failures": [str(error)],
        }
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
