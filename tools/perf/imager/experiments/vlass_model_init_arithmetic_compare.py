#!/usr/bin/env python3
"""Compare a CASA A-D model-init trace with exact casa-rs preparation arithmetic.

This is a bounded, read-only correctness diagnostic.  It reads the frozen CASA
and casa-rs model/weight images, explicitly narrows their pixels to Float, and
evaluates the current casa-rs implementation of CASA's promoted LEL expression:

    Float(Double(model) /
          (Double(Float(sqrt(abs(weight)))) /
           Double(Float(sqrt(max(weight))))))

The trace's stage D is receipted but deliberately not reconstructed here; FFT
semantics are a separate diagnostic.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import struct
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np


F32_SIGN_BIT = np.uint32(0x8000_0000)
F32_ALL_BITS = np.uint32(0xFFFF_FFFF)
TRACE_KIND = "vlass_casa_aw_model_initialization_trace"
MAX_RECORDED_MISMATCHES = 16
SEARCH_RANK_LIMIT = 32

SEARCH_PRECISIONS = ("f32", "f64")
DENOMINATOR_BOUNDARIES = ("rounded_f32", "retained_f64")
ALGEBRAIC_FORMS = (
    "divide_normalized_denominator",
    "multiply_scale_then_divide_root",
    "divide_root_then_multiply_scale",
    "multiply_by_scale_over_root",
    "scale_model_then_mask_over_root",
)


@dataclass(frozen=True)
class NativePlane:
    """A native CASA image plane narrowed exactly once to IEEE-754 Float."""

    values: np.ndarray
    receipt: dict[str, Any]


@dataclass(frozen=True)
class ArithmeticFormula:
    """One bounded combination of plausible CASA LEL evaluation boundaries."""

    sqrt_precision: str
    pb_scale_precision: str
    denominator_boundary: str
    mask_compare_precision: str
    model_mask_precision: str
    final_ratio_precision: str
    algebraic_form: str

    def label(self) -> str:
        return "__".join(
            (
                f"sqrt-{self.sqrt_precision}",
                f"pb-{self.pb_scale_precision}",
                f"deno-{self.denominator_boundary}",
                f"maskcmp-{self.mask_compare_precision}",
                f"modelmask-{self.model_mask_precision}",
                f"ratio-{self.final_ratio_precision}",
                f"form-{self.algebraic_form}",
            )
        )


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_payload(values: np.ndarray) -> str:
    """Hash canonical x-outer, y-contiguous little-endian Float payload."""

    if values.ndim != 2:
        raise RuntimeError(f"expected a two-dimensional plane, got {values.shape}")
    canonical = np.ascontiguousarray(values, dtype="<f4")
    return hashlib.sha256(canonical.tobytes(order="C")).hexdigest()


def image_content_sha256(pixels: np.ndarray, mask: np.ndarray) -> str:
    """Match the content-hash convention used by the A-D trace extractor."""

    digest = hashlib.sha256()
    for values in (np.ascontiguousarray(pixels), np.ascontiguousarray(mask)):
        digest.update(values.dtype.str.encode("ascii"))
        digest.update(np.asarray(values.shape, dtype=np.int64).tobytes())
        digest.update(values.tobytes())
    return digest.hexdigest()


def read_native_plane(
    path: Path,
    *,
    polarization: int,
    channel: int,
) -> NativePlane:
    try:
        from casatools import image
    except ImportError as error:
        raise RuntimeError(
            "casatools is required; run with the pinned CASA Python environment"
        ) from error

    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image {path}")
        reported_shape = [int(value) for value in tool.shape()]
        pixels = np.asarray(tool.getchunk())
        mask = np.asarray(tool.getchunk(getmask=True), dtype=np.bool_)
    finally:
        tool.close()

    if list(pixels.shape) != reported_shape:
        raise RuntimeError(
            f"{path} reported shape {reported_shape}, returned {pixels.shape}"
        )
    if pixels.ndim != 4:
        raise RuntimeError(f"expected CASA axes [x,y,pol,chan] at {path}: {pixels.shape}")
    if not 0 <= polarization < pixels.shape[2]:
        raise RuntimeError(f"polarization {polarization} is outside {pixels.shape}")
    if not 0 <= channel < pixels.shape[3]:
        raise RuntimeError(f"channel {channel} is outside {pixels.shape}")

    source_dtype = pixels.dtype.str
    source_content_sha256 = image_content_sha256(pixels, mask)
    source_plane = pixels[:, :, polarization, channel]
    source_mask = mask[:, :, polarization, channel]

    # casatools commonly returns Double even for a native Float image.  Rust's
    # PagedImage<f32> boundary narrows each pixel before any model arithmetic.
    narrowed = np.ascontiguousarray(source_plane, dtype=np.float32)
    if narrowed.dtype != np.dtype(np.float32):
        raise AssertionError("native-image narrowing did not produce Float")
    receipt = {
        "path": str(path),
        "reported_shape": reported_shape,
        "source_pixel_dtype": source_dtype,
        "source_image_content_sha256": source_content_sha256,
        "selection": {
            "polarization": polarization,
            "channel": channel,
            "shape": [int(value) for value in narrowed.shape],
            "mask_true": int(np.count_nonzero(source_mask)),
        },
        "narrowing": "casatools getchunk -> IEEE-754 f32 before arithmetic",
        "narrowed_scalar_dtype": "ieee754-f32",
        "narrowed_x_outer_y_contiguous_sha256": sha256_payload(narrowed),
    }
    return NativePlane(values=narrowed, receipt=receipt)


def f32_from_bits(bits: int) -> np.float32:
    return np.frombuffer(struct.pack("<I", bits), dtype="<f4")[0]


def f32_bits(value: np.float32 | float) -> int:
    return int(np.asarray(value, dtype="<f4").view("<u4"))


def f32_sqrt(value: np.float32) -> np.float32:
    result = np.sqrt(np.float32(value))
    return np.float32(result)


def weight_peak_f32(weight: np.ndarray) -> np.float32:
    if weight.dtype != np.dtype(np.float32):
        raise RuntimeError(f"weight must be narrowed to f32, got {weight.dtype}")
    # np.fmax has Rust f32::max's one-NaN behavior and preserves a Float result.
    peak = np.fmax.reduce(weight.ravel(order="C"), initial=np.float32(0.0))
    peak = np.float32(peak)
    if not (np.isfinite(peak) and peak > np.float32(0.0)):
        raise RuntimeError(f"weight peak is non-finite or non-positive: {peak!r}")
    return peak


def prepare_model_block(
    model: np.ndarray,
    weight: np.ndarray,
    *,
    pb_scale_factor: float,
    pb_limit: float,
) -> np.ndarray:
    """Evaluate the current Rust/CASA LEL arithmetic with explicit promotions."""

    if model.shape != weight.shape:
        raise RuntimeError(f"model/weight shapes disagree: {model.shape} != {weight.shape}")
    if model.dtype != np.dtype(np.float32) or weight.dtype != np.dtype(np.float32):
        raise RuntimeError("model and weight must be f32 before model preparation")
    if not (math.isfinite(pb_scale_factor) and pb_scale_factor > 0.0):
        raise RuntimeError(f"invalid f64 PB scale: {pb_scale_factor!r}")
    if not (math.isfinite(pb_limit) and pb_limit >= 0.0):
        raise RuntimeError(f"invalid f64 PB limit: {pb_limit!r}")

    # sqrt(abs(weight)) is computed and rounded as Float.  Only then is it
    # promoted to Double for both the denominator division and model division.
    root_f32 = np.asarray(np.sqrt(np.abs(weight)), dtype=np.float32)
    denominator_f64 = np.asarray(root_f32, dtype=np.float64) / np.float64(
        pb_scale_factor
    )
    valid = np.isfinite(denominator_f64) & (
        denominator_f64 > np.float64(pb_limit)
    )
    result_f32 = np.zeros(model.shape, dtype=np.float32)
    quotient_f64 = np.asarray(model[valid], dtype=np.float64) / denominator_f64[valid]
    result_f32[valid] = np.asarray(quotient_f64, dtype=np.float32)
    return result_f32


def ordered_f32_bits(bits: np.ndarray) -> np.ndarray:
    """Map Float bit patterns to monotonically ordered unsigned integers."""

    bits = np.asarray(bits, dtype=np.uint32)
    negative = (bits & F32_SIGN_BIT) != 0
    return np.where(
        negative,
        (~bits) & F32_ALL_BITS,
        bits | F32_SIGN_BIT,
    ).astype(np.uint64, copy=False)


def json_float(value: np.float32) -> float | str:
    converted = float(value)
    if math.isnan(converted):
        return "nan"
    if math.isinf(converted):
        return "+inf" if converted > 0 else "-inf"
    return converted


def mismatch_entry(
    candidate: np.float32,
    reference: np.float32,
    *,
    x: int,
    y: int,
) -> dict[str, Any]:
    candidate_bits = f32_bits(candidate)
    reference_bits = f32_bits(reference)
    candidate_order = int(ordered_f32_bits(np.asarray([candidate_bits]))[0])
    reference_order = int(ordered_f32_bits(np.asarray([reference_bits]))[0])
    ulp_meaningful = not (np.isnan(candidate) or np.isnan(reference))
    return {
        "coordinate_xy": [x, y],
        "candidate": json_float(candidate),
        "reference": json_float(reference),
        "candidate_f32_bits_hex": f"0x{candidate_bits:08x}",
        "reference_f32_bits_hex": f"0x{reference_bits:08x}",
        "signed_ulp_delta_candidate_minus_reference": (
            candidate_order - reference_order if ulp_meaningful else None
        ),
        "absolute_ulp_delta": (
            abs(candidate_order - reference_order) if ulp_meaningful else None
        ),
    }


def compare_formula_to_a(
    model: np.ndarray,
    weight: np.ndarray,
    stage_a: np.ndarray,
    *,
    pb_limit: float,
    block_rows: int,
) -> dict[str, Any]:
    if model.shape != weight.shape or model.shape != stage_a.shape:
        raise RuntimeError(
            "formula inputs and stage A have different shapes: "
            f"{model.shape}, {weight.shape}, {stage_a.shape}"
        )
    peak_f32 = weight_peak_f32(weight)
    pb_scale_f32 = f32_sqrt(peak_f32)
    pb_scale_f64 = float(pb_scale_f32)
    candidate_digest = hashlib.sha256()
    mismatch_count = 0
    first_mismatches: list[dict[str, Any]] = []
    nx, ny = model.shape

    for x0 in range(0, nx, block_rows):
        x1 = min(x0 + block_rows, nx)
        candidate = prepare_model_block(
            model[x0:x1, :],
            weight[x0:x1, :],
            pb_scale_factor=pb_scale_f64,
            pb_limit=pb_limit,
        )
        candidate_digest.update(
            np.ascontiguousarray(candidate, dtype="<f4").tobytes(order="C")
        )
        reference = np.asarray(stage_a[x0:x1, :], dtype=np.float32)
        candidate_bits = candidate.view(np.uint32)
        reference_bits = reference.view(np.uint32)
        mismatches = candidate_bits != reference_bits
        mismatch_count += int(np.count_nonzero(mismatches))
        remaining = MAX_RECORDED_MISMATCHES - len(first_mismatches)
        if remaining > 0 and np.any(mismatches):
            for flat_index in np.flatnonzero(mismatches.ravel(order="C"))[:remaining]:
                local_x, y = divmod(int(flat_index), ny)
                first_mismatches.append(
                    mismatch_entry(
                        candidate[local_x, y],
                        reference[local_x, y],
                        x=x0 + local_x,
                        y=y,
                    )
                )

    return {
        "weight_peak_f32_bits_hex": f"0x{f32_bits(peak_f32):08x}",
        "pb_scale": {
            "formula": "f64(f32(sqrt(max(f32 weight))))",
            "f32_bits_hex": f"0x{f32_bits(pb_scale_f32):08x}",
            "f64_bits_hex": (
                f"0x{struct.unpack('<Q', struct.pack('<d', pb_scale_f64))[0]:016x}"
            ),
        },
        "candidate_x_outer_y_contiguous_sha256": candidate_digest.hexdigest(),
        "versus_casa_stage_a": {
            "comparison": "IEEE-754 f32 bits",
            "pixels": nx * ny,
            "bitwise_mismatch_count": mismatch_count,
            "bitwise_equal": mismatch_count == 0,
            "first_mismatches_x_outer_y_inner": first_mismatches,
        },
    }


def compare_existing_plane_to_a(
    candidate: np.ndarray,
    stage_a: np.ndarray,
    *,
    block_rows: int,
) -> dict[str, Any]:
    if candidate.shape != stage_a.shape:
        raise RuntimeError(
            f"candidate/stage-A shapes disagree: {candidate.shape} != {stage_a.shape}"
        )
    mismatch_count = 0
    first_mismatches: list[dict[str, Any]] = []
    nx, ny = stage_a.shape
    for x0 in range(0, nx, block_rows):
        x1 = min(x0 + block_rows, nx)
        candidate_block = np.asarray(candidate[x0:x1, :], dtype=np.float32)
        reference_block = np.asarray(stage_a[x0:x1, :], dtype=np.float32)
        mismatches = candidate_block.view(np.uint32) != reference_block.view(np.uint32)
        mismatch_count += int(np.count_nonzero(mismatches))
        remaining = MAX_RECORDED_MISMATCHES - len(first_mismatches)
        if remaining > 0 and np.any(mismatches):
            for flat_index in np.flatnonzero(mismatches.ravel(order="C"))[:remaining]:
                local_x, y = divmod(int(flat_index), ny)
                first_mismatches.append(
                    mismatch_entry(
                        candidate_block[local_x, y],
                        reference_block[local_x, y],
                        x=x0 + local_x,
                        y=y,
                    )
                )
    return {
        "comparison": "IEEE-754 f32 bits",
        "pixels": nx * ny,
        "bitwise_mismatch_count": mismatch_count,
        "bitwise_equal": mismatch_count == 0,
        "first_mismatches_x_outer_y_inner": first_mismatches,
    }


def verify_imaginary_zero(
    imaginary: np.ndarray,
    *,
    block_rows: int,
) -> dict[str, Any]:
    nx, ny = imaginary.shape
    numeric_nonzero_count = 0
    nonpositive_zero_bits_count = 0
    first_numeric_nonzero: list[dict[str, Any]] = []
    positive_zero_bits = np.uint32(0)
    for x0 in range(0, nx, block_rows):
        x1 = min(x0 + block_rows, nx)
        block = np.asarray(imaginary[x0:x1, :], dtype=np.float32)
        numeric_nonzero = block != np.float32(0.0)
        bits = block.view(np.uint32)
        numeric_nonzero_count += int(np.count_nonzero(numeric_nonzero))
        nonpositive_zero_bits_count += int(np.count_nonzero(bits != positive_zero_bits))
        remaining = MAX_RECORDED_MISMATCHES - len(first_numeric_nonzero)
        if remaining > 0 and np.any(numeric_nonzero):
            for flat_index in np.flatnonzero(numeric_nonzero.ravel(order="C"))[:remaining]:
                local_x, y = divmod(int(flat_index), ny)
                value = block[local_x, y]
                first_numeric_nonzero.append(
                    {
                        "coordinate_xy": [x0 + local_x, y],
                        "value": json_float(value),
                        "f32_bits_hex": f"0x{f32_bits(value):08x}",
                    }
                )
    return {
        "pixels": nx * ny,
        "numeric_nonzero_count": numeric_nonzero_count,
        "all_numerically_zero": numeric_nonzero_count == 0,
        "non_positive_zero_bit_pattern_count": nonpositive_zero_bits_count,
        "all_positive_zero_bits": nonpositive_zero_bits_count == 0,
        "first_numeric_nonzero_x_outer_y_inner": first_numeric_nonzero,
    }


def load_trace(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("kind") != TRACE_KIND:
        raise RuntimeError(f"{path} is not a {TRACE_KIND} receipt")
    return value


def raw_stage_inputs(
    trace: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], tuple[int, int]]:
    stages = trace.get("stages")
    if not isinstance(stages, list):
        raise RuntimeError("trace receipt lacks a stages array")
    by_name = {
        str(stage.get("stage")): stage
        for stage in stages
        if isinstance(stage, dict)
    }
    if set(by_name) != {"A", "B", "C", "D"}:
        raise RuntimeError(f"trace stages are not exactly A-D: {sorted(by_name)}")

    shape: tuple[int, int] | None = None
    inputs: dict[str, dict[str, Any]] = {}
    for name in ("A", "B", "C", "D"):
        stage = by_name[name]
        selected = stage.get("selected_plane")
        if not isinstance(selected, dict) or not isinstance(selected.get("shape"), list):
            raise RuntimeError(f"stage {name} lacks a selected-plane shape")
        stage_shape = tuple(int(value) for value in selected["shape"])
        if len(stage_shape) != 2:
            raise RuntimeError(f"stage {name} is not two-dimensional: {stage_shape}")
        if shape is None:
            shape = stage_shape
        elif stage_shape != shape:
            raise RuntimeError(f"stage {name} shape {stage_shape} differs from {shape}")
        layout = stage.get("raw_layout")
        if not isinstance(layout, dict) or layout.get("contiguous_axis") != "y":
            raise RuntimeError(f"stage {name} is not y-contiguous")
        raw_path = Path(str(stage.get("raw_path")))
        actual_sha256 = sha256_path(raw_path)
        expected_sha256 = str(stage.get("raw_sha256"))
        if actual_sha256 != expected_sha256:
            raise RuntimeError(
                f"stage {name} hash mismatch: {actual_sha256} != {expected_sha256}"
            )
        inputs[name] = {
            "path": str(raw_path),
            "bytes": raw_path.stat().st_size,
            "sha256": actual_sha256,
            "shape": list(stage_shape),
            "components": list(stage.get("raw_components", [])),
            "layout": layout,
        }
    assert shape is not None
    return inputs, shape


def memmap_trace_stages(
    raw_inputs: dict[str, dict[str, Any]],
    shape: tuple[int, int],
) -> dict[str, np.memmap]:
    nx, ny = shape
    expected_sizes = {
        "A": nx * ny * np.dtype("<f4").itemsize,
        "B": nx * ny * 2 * np.dtype("<f4").itemsize,
        "C": nx * ny * 2 * np.dtype("<f4").itemsize,
        "D": nx * ny * 2 * np.dtype("<f4").itemsize,
    }
    result: dict[str, np.memmap] = {}
    for name in ("A", "B", "C", "D"):
        if raw_inputs[name]["bytes"] != expected_sizes[name]:
            raise RuntimeError(
                f"stage {name} has {raw_inputs[name]['bytes']} bytes, "
                f"expected {expected_sizes[name]}"
            )
        map_shape = shape if name == "A" else (nx, ny, 2)
        result[name] = np.memmap(
            raw_inputs[name]["path"],
            dtype="<f4",
            mode="r",
            shape=map_shape,
            order="C",
        )
    return result


def trace_provenance_matches(
    trace: dict[str, Any],
    native_inputs: dict[str, NativePlane],
) -> dict[str, Any]:
    try:
        normalization = trace["prediction_run"]["normalization_inputs"]
        trace_model_sha = str(normalization["model"]["content_sha256"])
        trace_weight_sha = str(normalization["weight"]["content_sha256"])
    except (KeyError, TypeError) as error:
        raise RuntimeError("trace receipt lacks normalization input hashes") from error

    model_matches = [
        label
        for label in ("casa_model", "rust_model", "trace_model")
        if native_inputs[label].receipt["source_image_content_sha256"] == trace_model_sha
    ]
    weight_matches = [
        label
        for label in ("casa_weight", "rust_weight", "trace_weight")
        if native_inputs[label].receipt["source_image_content_sha256"] == trace_weight_sha
    ]
    return {
        "trace_model_content_sha256": trace_model_sha,
        "matching_supplied_model_inputs": model_matches,
        "trace_weight_content_sha256": trace_weight_sha,
        "matching_supplied_weight_inputs": weight_matches,
        "matching_candidate_combinations": [
            f"{model_label}_{weight_label}"
            for model_label in model_matches
            for weight_label in weight_matches
        ],
    }


def precision_dtype(precision: str) -> np.dtype[Any]:
    if precision == "f32":
        return np.dtype(np.float32)
    if precision == "f64":
        return np.dtype(np.float64)
    raise RuntimeError(f"unknown arithmetic precision {precision!r}")


def cast_precision(values: Any, precision: str) -> np.ndarray:
    return np.asarray(values, dtype=precision_dtype(precision))


def binary_precision(
    operation: str,
    left: Any,
    right: Any,
    precision: str,
) -> np.ndarray:
    dtype = precision_dtype(precision)
    left_value = np.asarray(left, dtype=dtype)
    right_value = np.asarray(right, dtype=dtype)
    if operation == "add":
        result = np.add(left_value, right_value)
    elif operation == "multiply":
        result = np.multiply(left_value, right_value)
    elif operation == "divide":
        result = np.divide(left_value, right_value)
    else:
        raise RuntimeError(f"unknown binary operation {operation!r}")
    narrowed = np.asarray(result, dtype=dtype)
    if narrowed.dtype != dtype:
        raise AssertionError(f"{operation} did not preserve {precision}")
    return narrowed


def sqrt_precision(values: np.ndarray, precision: str) -> np.ndarray:
    dtype = precision_dtype(precision)
    narrowed = np.asarray(values, dtype=dtype)
    result = np.asarray(np.sqrt(np.abs(narrowed)), dtype=dtype)
    if result.dtype != dtype:
        raise AssertionError(f"sqrt did not preserve {precision}")
    return result


def evaluate_arithmetic_formula(
    model_f32: np.ndarray,
    weight_f32: np.ndarray,
    *,
    pb_scale_f64: float,
    pb_limit_f32: np.float32,
    formula: ArithmeticFormula,
) -> np.ndarray:
    """Evaluate one formula at selected pixels and narrow its result to Float."""

    if model_f32.dtype != np.dtype(np.float32):
        raise RuntimeError("arithmetic-search model is not f32")
    if weight_f32.dtype != np.dtype(np.float32):
        raise RuntimeError("arithmetic-search weight is not f32")
    if model_f32.shape != weight_f32.shape:
        raise RuntimeError("arithmetic-search model/weight shapes differ")

    root = sqrt_precision(weight_f32, formula.sqrt_precision)
    pb_scale = cast_precision(pb_scale_f64, formula.pb_scale_precision)
    denominator_division_precision = (
        "f64"
        if "f64" in (formula.sqrt_precision, formula.pb_scale_precision)
        else "f32"
    )
    denominator_unstored = binary_precision(
        "divide",
        root,
        pb_scale,
        denominator_division_precision,
    )
    denominator = cast_precision(
        denominator_unstored,
        "f32"
        if formula.denominator_boundary == "rounded_f32"
        else "f64",
    )

    compare_denominator = cast_precision(
        denominator, formula.mask_compare_precision
    )
    compare_limit = cast_precision(pb_limit_f32, formula.mask_compare_precision)
    mask_bool = compare_denominator > compare_limit
    mask = cast_precision(mask_bool, formula.model_mask_precision)
    model_masked = binary_precision(
        "multiply",
        model_f32,
        mask,
        formula.model_mask_precision,
    )

    ratio_precision = formula.final_ratio_precision
    mask_inverse = cast_precision(~mask_bool, ratio_precision)
    numerator = cast_precision(model_masked, ratio_precision)
    if formula.algebraic_form == "divide_normalized_denominator":
        safe_denominator = binary_precision(
            "add",
            denominator,
            mask_inverse,
            ratio_precision,
        )
        ratio = binary_precision(
            "divide",
            numerator,
            safe_denominator,
            ratio_precision,
        )
    else:
        root_for_ratio = cast_precision(root, ratio_precision)
        scale_for_ratio = cast_precision(pb_scale, ratio_precision)
        inverse_scale = binary_precision(
            "multiply",
            mask_inverse,
            scale_for_ratio,
            ratio_precision,
        )
        safe_root = binary_precision(
            "add",
            root_for_ratio,
            inverse_scale,
            ratio_precision,
        )
        if formula.algebraic_form == "multiply_scale_then_divide_root":
            scaled_numerator = binary_precision(
                "multiply",
                numerator,
                scale_for_ratio,
                ratio_precision,
            )
            ratio = binary_precision(
                "divide",
                scaled_numerator,
                safe_root,
                ratio_precision,
            )
        elif formula.algebraic_form == "divide_root_then_multiply_scale":
            unscaled_ratio = binary_precision(
                "divide",
                numerator,
                safe_root,
                ratio_precision,
            )
            ratio = binary_precision(
                "multiply",
                unscaled_ratio,
                scale_for_ratio,
                ratio_precision,
            )
        elif formula.algebraic_form == "multiply_by_scale_over_root":
            reciprocal_factor = binary_precision(
                "divide",
                scale_for_ratio,
                safe_root,
                ratio_precision,
            )
            ratio = binary_precision(
                "multiply",
                numerator,
                reciprocal_factor,
                ratio_precision,
            )
        elif formula.algebraic_form == "scale_model_then_mask_over_root":
            model_scale = cast_precision(pb_scale, formula.model_mask_precision)
            scaled_model = binary_precision(
                "multiply",
                model_f32,
                model_scale,
                formula.model_mask_precision,
            )
            scaled_masked = binary_precision(
                "multiply",
                scaled_model,
                mask,
                formula.model_mask_precision,
            )
            ratio = binary_precision(
                "divide",
                cast_precision(scaled_masked, ratio_precision),
                safe_root,
                ratio_precision,
            )
        else:
            raise RuntimeError(
                f"unknown algebraic form {formula.algebraic_form!r}"
            )
    return np.asarray(ratio, dtype=np.float32)


def arithmetic_formula_space() -> list[ArithmeticFormula]:
    return [
        ArithmeticFormula(*values)
        for values in itertools.product(
            SEARCH_PRECISIONS,
            SEARCH_PRECISIONS,
            DENOMINATOR_BOUNDARIES,
            SEARCH_PRECISIONS,
            SEARCH_PRECISIONS,
            SEARCH_PRECISIONS,
            ALGEBRAIC_FORMS,
        )
    ]


def first_vector_mismatch(
    candidate: np.ndarray,
    reference: np.ndarray,
    x_coordinates: np.ndarray,
    y_coordinates: np.ndarray,
) -> dict[str, Any] | None:
    mismatches = candidate.view(np.uint32) != reference.view(np.uint32)
    indices = np.flatnonzero(mismatches)
    if indices.size == 0:
        return None
    index = int(indices[0])
    return mismatch_entry(
        candidate[index],
        reference[index],
        x=int(x_coordinates[index]),
        y=int(y_coordinates[index]),
    )


def exhaustive_arithmetic_search(
    model: np.ndarray,
    weight: np.ndarray,
    stage_a: np.ndarray,
    *,
    pb_scale_f64: float,
    pb_limit_f32: np.float32,
) -> dict[str, Any]:
    """Search all requested boundaries on a proof-complete sparse domain."""

    if model.shape != weight.shape or model.shape != stage_a.shape:
        raise RuntimeError("arithmetic-search input shapes differ")
    model_bits = model.view(np.uint32)
    reference_bits = np.asarray(stage_a, dtype=np.float32).view(np.uint32)
    nonfinite_weight = ~np.isfinite(weight)
    search_domain = (
        (model_bits != np.uint32(0))
        | (reference_bits != np.uint32(0))
        | nonfinite_weight
    )
    x_coordinates, y_coordinates = np.nonzero(search_domain)
    selected_model = np.asarray(
        model[x_coordinates, y_coordinates], dtype=np.float32
    )
    selected_weight = np.asarray(
        weight[x_coordinates, y_coordinates], dtype=np.float32
    )
    selected_reference = np.asarray(
        stage_a[x_coordinates, y_coordinates], dtype=np.float32
    )
    if selected_model.size == 0:
        raise RuntimeError("arithmetic-search support is empty")

    variants: list[dict[str, Any]] = []
    unique_outcomes: dict[str, dict[str, Any]] = {}
    exact_matches: list[dict[str, Any]] = []
    for formula in arithmetic_formula_space():
        candidate = evaluate_arithmetic_formula(
            selected_model,
            selected_weight,
            pb_scale_f64=pb_scale_f64,
            pb_limit_f32=pb_limit_f32,
            formula=formula,
        )
        candidate_bits = candidate.view(np.uint32)
        mismatch_count = int(np.count_nonzero(candidate_bits != selected_reference.view(np.uint32)))
        candidate_sha256 = hashlib.sha256(
            np.ascontiguousarray(candidate, dtype="<f4").tobytes(order="C")
        ).hexdigest()
        first_mismatch = first_vector_mismatch(
            candidate,
            selected_reference,
            x_coordinates,
            y_coordinates,
        )
        summary = {
            "label": formula.label(),
            "formula": asdict(formula),
            "full_plane_bitwise_mismatch_count": mismatch_count,
            "full_plane_bitwise_equal": mismatch_count == 0,
            "candidate_search_domain_sha256": candidate_sha256,
            "first_mismatch": first_mismatch,
        }
        variants.append(summary)
        if mismatch_count == 0:
            exact_matches.append(summary)

        outcome_key = f"{mismatch_count}:{candidate_sha256}"
        if outcome_key not in unique_outcomes:
            unique_outcomes[outcome_key] = {
                "full_plane_bitwise_mismatch_count": mismatch_count,
                "candidate_search_domain_sha256": candidate_sha256,
                "first_mismatch": first_mismatch,
                "formula_labels": [],
            }
        unique_outcomes[outcome_key]["formula_labels"].append(formula.label())

    variants.sort(
        key=lambda item: (
            item["full_plane_bitwise_mismatch_count"],
            item["label"],
        )
    )
    outcomes = sorted(
        unique_outcomes.values(),
        key=lambda item: (
            item["full_plane_bitwise_mismatch_count"],
            item["candidate_search_domain_sha256"],
        ),
    )
    for outcome in outcomes:
        outcome["formula_count"] = len(outcome["formula_labels"])

    casa_source_formula = ArithmeticFormula(
        sqrt_precision="f32",
        pb_scale_precision="f64",
        denominator_boundary="rounded_f32",
        mask_compare_precision="f32",
        model_mask_precision="f32",
        final_ratio_precision="f32",
        algebraic_form="divide_normalized_denominator",
    )
    casa_source_result = next(
        item for item in variants if item["label"] == casa_source_formula.label()
    )
    full_pixels = model.shape[0] * model.shape[1]
    return {
        "search_contract": {
            "primary_inputs": (
                "immutable original v55 casa-rs model and exact trace/CASA weight"
            ),
            "search_domain": (
                "union of non-positive-zero model pixels, non-positive-zero "
                "stage-A pixels, and non-finite weight pixels"
            ),
            "outside_domain_proof": (
                "model and stage A are positive zero and weight is finite; every "
                "enumerated masked formula therefore returns positive zero"
            ),
            "full_pixels": full_pixels,
            "search_domain_pixels": int(selected_model.size),
            "outside_domain_pixels": full_pixels - int(selected_model.size),
            "nonfinite_weight_pixels": int(np.count_nonzero(nonfinite_weight)),
            "enumerated_axes": {
                "sqrt_precision": list(SEARCH_PRECISIONS),
                "pb_scale_precision": list(SEARCH_PRECISIONS),
                "denominator_boundary": list(DENOMINATOR_BOUNDARIES),
                "mask_compare_precision": list(SEARCH_PRECISIONS),
                "model_mask_precision": list(SEARCH_PRECISIONS),
                "final_ratio_precision": list(SEARCH_PRECISIONS),
                "algebraic_form": list(ALGEBRAIC_FORMS),
            },
            "variant_count": len(variants),
            "unique_candidate_outcomes": len(outcomes),
        },
        "casa_source_predicted_formula": casa_source_result,
        "exact_match_count": len(exact_matches),
        "exact_matches": exact_matches,
        "ranked_variants": variants,
        "ranked_unique_outcomes": outcomes[:SEARCH_RANK_LIMIT],
        "ranked_unique_outcomes_truncated": len(outcomes) > SEARCH_RANK_LIMIT,
    }


def casa_lel_source_receipt(source_root: Path | None) -> dict[str, Any]:
    findings = [
        {
            "relative_path": (
                "src/code/synthesis/ImagerObjects/SIImageStoreMultiTerm.cc"
            ),
            "lines": "973-994",
            "finding": (
                "PB scale is Double; sqrt(Float weight)/Double is explicitly "
                "materialized as LatticeExpr<Float> deno; mask, maskinv, and "
                "ratio are then LatticeExpr<Float>"
            ),
        },
        {
            "relative_path": "src/code/synthesis/ImagerObjects/SIImageStore.cc",
            "lines": "1446-1461",
            "finding": (
                "getPbMax builds sqrt(max(Float weight)) and returns getFloat "
                "through a Double return type"
            ),
        },
        {
            "relative_path": "src/code/synthesis/ImagerObjects/SIImageStore.h",
            "lines": "291-316",
            "finding": "getPbMax and itsPBScaleFactor are declared Double",
        },
        {
            "relative_path": "casacore/lattices/LEL/LatticeExprNode.cc",
            "lines": "2069-2121,2222-2247,2287-2312",
            "finding": (
                "binary operators promote to the higher operand type and expose "
                "Double-to-Float conversion nodes"
            ),
        },
        {
            "relative_path": "casacore/lattices/LEL/LatticeExpr.tcc",
            "lines": "79-102",
            "finding": (
                "constructing LatticeExpr<Float> from a Double expression calls "
                "makeFloat and therefore establishes the denominator rounding boundary"
            ),
        },
        {
            "relative_path": "casacore/lattices/LEL/LELBinary.tcc",
            "lines": "52-139",
            "finding": (
                "array add, multiply, divide, and scalar operations execute in "
                "the LELBinary template type"
            ),
        },
    ]
    result: dict[str, Any] = {
        "casatools_git_sha": "61020062cee290f5466cffed5ec5032e0c7a3434",
        "findings": findings,
    }
    if source_root is None:
        result["source_root"] = None
        result["source_hash_status"] = "not_requested"
        return result
    result["source_root"] = str(source_root)
    result["source_hash_status"] = "hashed"
    for finding in findings:
        path = source_root / finding["relative_path"]
        if not path.is_file():
            raise RuntimeError(f"CASA LEL source file is missing: {path}")
        finding["path"] = str(path)
        finding["sha256"] = sha256_path(path)
    return result


def run_self_test() -> None:
    model_value = f32_from_bits(0x3FCB_5F9B)
    weight_value = f32_from_bits(0x3F45_8F9D)
    weight_peak = f32_from_bits(0x3F9E_064B)
    model = np.asarray([[model_value, np.float32(0.0)]], dtype=np.float32)
    weight = np.asarray([[weight_value, weight_peak]], dtype=np.float32)
    peak = weight_peak_f32(weight)
    pb_scale = float(f32_sqrt(peak))
    prepared = prepare_model_block(
        model,
        weight,
        pb_scale_factor=pb_scale,
        pb_limit=float(np.float32(0.0001)),
    )
    assert f32_bits(prepared[0, 0]) == 0x4000_9D64

    rounded_denominator = np.float32(
        f32_sqrt(np.abs(weight_value)) / f32_sqrt(weight_peak)
    )
    rounded_result = np.float32(model_value / rounded_denominator)
    assert f32_bits(rounded_result) == 0x4000_9D65

    orientation = np.asarray(
        [[10.0, 11.0, 12.0], [20.0, 21.0, 22.0]],
        dtype=np.float32,
    )
    serialized = np.frombuffer(
        np.ascontiguousarray(orientation, dtype="<f4").tobytes(order="C"),
        dtype="<f4",
    )
    assert serialized.tolist() == [10.0, 11.0, 12.0, 20.0, 21.0, 22.0]

    adjacent = mismatch_entry(
        f32_from_bits(0x4000_9D65),
        f32_from_bits(0x4000_9D64),
        x=3,
        y=5,
    )
    assert adjacent["signed_ulp_delta_candidate_minus_reference"] == 1
    assert adjacent["absolute_ulp_delta"] == 1

    source_formula = ArithmeticFormula(
        sqrt_precision="f32",
        pb_scale_precision="f64",
        denominator_boundary="rounded_f32",
        mask_compare_precision="f32",
        model_mask_precision="f32",
        final_ratio_precision="f32",
        algebraic_form="divide_normalized_denominator",
    )
    source_prepared = evaluate_arithmetic_formula(
        model.ravel(),
        weight.ravel(),
        pb_scale_f64=pb_scale,
        pb_limit_f32=np.float32(0.0001),
        formula=source_formula,
    )
    assert f32_bits(source_prepared[0]) == 0x4000_9D65
    assert len(arithmetic_formula_space()) == 320
    print("vlass_model_init_arithmetic_compare self-test: PASS", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-receipt", type=Path)
    parser.add_argument("--casa-prefix", type=Path)
    parser.add_argument("--rust-prefix", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--casa-source-root",
        type=Path,
        help=(
            "optional casatools/src/code root used to hash the inspected CASA/LEL source"
        ),
    )
    parser.add_argument("--block-rows", type=int, default=64)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        run_self_test()
        return
    required = {
        "--trace-receipt": args.trace_receipt,
        "--casa-prefix": args.casa_prefix,
        "--rust-prefix": args.rust_prefix,
        "--output": args.output,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        parser.error("required outside --self-test: " + ", ".join(missing))
    if args.block_rows <= 0:
        parser.error("--block-rows must be positive")

    assert args.trace_receipt is not None
    assert args.casa_prefix is not None
    assert args.rust_prefix is not None
    assert args.output is not None
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {args.output}")

    trace = load_trace(args.trace_receipt)
    selection = trace.get("selection")
    if not isinstance(selection, dict):
        raise RuntimeError("trace receipt lacks a selection object")
    term = int(selection["term"])
    polarization = int(selection["polarization"])
    channel = int(selection["channel"])
    if min(term, polarization, channel) < 0:
        raise RuntimeError("trace term, polarization, and channel must be non-negative")

    raw_inputs, trace_shape = raw_stage_inputs(trace)
    stages = memmap_trace_stages(raw_inputs, trace_shape)

    try:
        normalization_inputs = trace["prediction_run"]["normalization_inputs"]
        trace_model_path = Path(str(normalization_inputs["model"]["path"]))
        trace_weight_path = Path(str(normalization_inputs["weight"]["path"]))
    except (KeyError, TypeError) as error:
        raise RuntimeError("trace receipt lacks normalization input paths") from error
    native_paths = {
        "casa_model": Path(f"{args.casa_prefix}.model.tt{term}"),
        "casa_weight": Path(f"{args.casa_prefix}.weight.tt0"),
        "rust_model": Path(f"{args.rust_prefix}.model.tt{term}"),
        "rust_weight": Path(f"{args.rust_prefix}.weight.tt0"),
        # Stage A was evaluated from these exact cloned images.  Keep them
        # alongside the original CASA/casa-rs products so clone or post-run
        # drift cannot be mistaken for an arithmetic difference.
        "trace_model": trace_model_path,
        "trace_weight": trace_weight_path,
    }
    native_inputs = {
        label: read_native_plane(
            path,
            polarization=polarization,
            channel=channel,
        )
        for label, path in native_paths.items()
    }
    for label, native in native_inputs.items():
        if native.values.shape != trace_shape:
            raise RuntimeError(
                f"{label} shape {native.values.shape} differs from trace {trace_shape}"
            )

    a_metadata = trace.get("a_metadata")
    if not isinstance(a_metadata, dict) or "pblimit_f32_bits" not in a_metadata:
        raise RuntimeError("trace receipt lacks stage-A pblimit bits")
    pb_limit_f32 = f32_from_bits(int(a_metadata["pblimit_f32_bits"]))
    pb_limit_f64 = float(abs(pb_limit_f32))

    combinations = {
        "casa_model_casa_weight": ("casa_model", "casa_weight"),
        "rust_model_rust_weight": ("rust_model", "rust_weight"),
        "rust_model_casa_weight": ("rust_model", "casa_weight"),
        "casa_model_rust_weight": ("casa_model", "rust_weight"),
        "post_run_trace_model_trace_weight_negative_lifecycle": (
            "trace_model",
            "trace_weight",
        ),
    }
    arithmetic_results: dict[str, Any] = {}
    for label, (model_label, weight_label) in combinations.items():
        comparison = compare_formula_to_a(
            native_inputs[model_label].values,
            native_inputs[weight_label].values,
            stages["A"],
            pb_limit=pb_limit_f64,
            block_rows=args.block_rows,
        )
        comparison["model_input"] = model_label
        comparison["weight_input"] = weight_label
        arithmetic_results[label] = comparison

    stage_transfer: dict[str, Any] = {}
    for stage_name in ("B", "C"):
        complex_plane = stages[stage_name]
        real_comparison = compare_existing_plane_to_a(
            complex_plane[..., 0],
            stages["A"],
            block_rows=args.block_rows,
        )
        imaginary_zero = verify_imaginary_zero(
            complex_plane[..., 1],
            block_rows=args.block_rows,
        )
        stage_transfer[stage_name] = {
            "real_versus_stage_a": real_comparison,
            "imaginary": imaginary_zero,
            "verified_real_equals_a_and_imaginary_zero": (
                real_comparison["bitwise_equal"]
                and imaginary_zero["all_numerically_zero"]
            ),
        }

    exact_arithmetic_candidates = [
        label
        for label, comparison in arithmetic_results.items()
        if comparison["versus_casa_stage_a"]["bitwise_equal"]
    ]
    pb_scale_f64_bits = int(a_metadata["pb_scale_factor_f64_bits"])
    pb_scale_f64 = struct.unpack("<d", struct.pack("<Q", pb_scale_f64_bits))[0]
    arithmetic_search = exhaustive_arithmetic_search(
        native_inputs["rust_model"].values,
        native_inputs["trace_weight"].values,
        stages["A"],
        pb_scale_f64=pb_scale_f64,
        pb_limit_f32=pb_limit_f32,
    )
    post_run_clone_lifecycle = {
        "chronology": (
            "CASA copied the immutable v55 Rust model before stage A, then "
            "mutated the cloned output model during its divide/predict/multiply "
            "lifecycle; the clone is not a valid stage-A input"
        ),
        "post_run_trace_model_versus_immutable_v55_rust_model": (
            compare_existing_plane_to_a(
                native_inputs["trace_model"].values,
                native_inputs["rust_model"].values,
                block_rows=args.block_rows,
            )
        ),
        "trace_weight_versus_frozen_casa_weight": compare_existing_plane_to_a(
            native_inputs["trace_weight"].values,
            native_inputs["casa_weight"].values,
            block_rows=args.block_rows,
        ),
    }
    result = {
        "kind": "vlass_model_init_arithmetic_comparison",
        "role": "bounded_correctness_diagnostic_not_promotion_evidence",
        "schema_version": 1,
        "trace_receipt": {
            "path": str(args.trace_receipt),
            "sha256": sha256_path(args.trace_receipt),
            "kind": trace.get("kind"),
            "casa": trace.get("casa"),
            "casatools_git_sha": trace.get("casatools_git_sha"),
        },
        "selection": {
            "term": term,
            "polarization": polarization,
            "channel": channel,
            "shape": list(trace_shape),
        },
        "canonical_serialization": {
            "scalar_dtype": "ieee754-f32-little-endian",
            "logical_axes": ["x", "y"],
            "contiguous_axis": "y",
            "offset": "(x * ny) + y",
        },
        "arithmetic": {
            "model_and_weight_boundary": (
                "each native image pixel explicitly narrowed to f32 before arithmetic"
            ),
            "pb_scale": "f64(f32(sqrt(max(f32 weight))))",
            "denominator": (
                "f64(f32(sqrt(abs(f32 weight)))) / pb_scale_f64"
            ),
            "division": "f64(f32 model) / denominator_f64",
            "result": "one final f32 narrowing",
            "pb_limit": "f64(abs(f32 trace pblimit)) with strict denominator > limit",
            "pb_limit_f32_bits_hex": f"0x{f32_bits(pb_limit_f32):08x}",
            "pb_limit_f64": pb_limit_f64,
        },
        "inputs": {
            "trace_raw_stages": raw_inputs,
            "native_images": {
                label: native.receipt for label, native in native_inputs.items()
            },
            "trace_normalization_provenance": trace_provenance_matches(
                trace, native_inputs
            ),
        },
        "arithmetic_candidates": arithmetic_results,
        "exact_stage_a_candidates": exact_arithmetic_candidates,
        "exhaustive_arithmetic_search": arithmetic_search,
        "post_run_clone_negative_lifecycle_receipt": post_run_clone_lifecycle,
        "casa_lel_source_evidence": casa_lel_source_receipt(args.casa_source_root),
        "casa_stage_transfer": stage_transfer,
        "stage_d": {
            "status": "receipted_not_compared",
            "reason": (
                "FFT reconstruction is intentionally outside this arithmetic diagnostic"
            ),
            "input": raw_inputs["D"],
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    print(json.dumps(result, sort_keys=True, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
