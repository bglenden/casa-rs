#!/usr/bin/env python3
"""Compare representative Rust/CASA visibility-column writes without loading a full MS."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np


FLOAT32_WEIGHT_ROUNDTRIP_RELATIVE_TOLERANCE = 2.0 * float(np.finfo(np.float32).eps)
FLOAT32_WEIGHT_ROUNDTRIP_ABSOLUTE_TOLERANCE = 0.0


def new_table():
    from casatools import table as table_tool

    return table_tool()


def arrays_bitwise_equal(left: np.ndarray, right: np.ndarray) -> bool:
    left = np.asarray(left)
    right = np.asarray(right)
    return (
        left.dtype == right.dtype
        and left.shape == right.shape
        and np.ascontiguousarray(left).tobytes()
        == np.ascontiguousarray(right).tobytes()
    )


def float32_weight_roundtrip_metrics(
    source: np.ndarray, candidate: np.ndarray
) -> dict[str, float | bool]:
    source = np.asarray(source)
    candidate = np.asarray(candidate)
    if source.shape != candidate.shape:
        return {
            "finite": False,
            "within_tolerance": False,
            "maximum_absolute_difference": math.inf,
            "maximum_relative_difference": math.inf,
        }

    absolute_difference = np.abs(candidate - source)
    relative_difference = np.zeros_like(absolute_difference, dtype=np.float64)
    nonzero_source = source != 0.0
    np.divide(
        absolute_difference,
        np.abs(source),
        out=relative_difference,
        where=nonzero_source,
    )
    relative_difference[~nonzero_source & (absolute_difference != 0.0)] = math.inf
    finite = bool(np.all(np.isfinite(candidate)))
    return {
        "finite": finite,
        "within_tolerance": finite
        and bool(
            np.allclose(
                source,
                candidate,
                rtol=FLOAT32_WEIGHT_ROUNDTRIP_RELATIVE_TOLERANCE,
                atol=FLOAT32_WEIGHT_ROUNDTRIP_ABSOLUTE_TOLERANCE,
                equal_nan=False,
            )
        ),
        "maximum_absolute_difference": float(np.max(absolute_difference))
        if absolute_difference.size
        else 0.0,
        "maximum_relative_difference": float(np.max(relative_difference))
        if relative_difference.size
        else 0.0,
    }


def casa_weight_contract_satisfied(
    comparison_key: str,
    *,
    bitwise_unchanged: bool,
    within_float32_roundtrip_tolerance: bool,
) -> bool:
    if comparison_key == "continuum_residual":
        return within_float32_roundtrip_tolerance
    return bitwise_unchanged


def selected_data_description_ids(ms: Path, spectral_windows: set[int]) -> list[int]:
    table = new_table()
    try:
        table.open(str(ms / "DATA_DESCRIPTION"), nomodify=True)
        values = np.asarray(table.getcol("SPECTRAL_WINDOW_ID"), dtype=np.int64)
    finally:
        table.close()
    return [
        int(index)
        for index, value in enumerate(values)
        if int(value) in spectral_windows
    ]


def parse_spectral_windows(selection: str) -> set[int]:
    result: set[int] = set()
    for item in selection.split(","):
        bounds = item.split(":", 1)[0].split("~")
        start = int(bounds[0])
        end = int(bounds[-1])
        result.update(range(start, end + 1))
    return result


def open_selection(ms: Path, field: int, data_description_ids: list[int]):
    table = new_table()
    table.open(str(ms), nomodify=True)
    ids = ",".join(str(value) for value in data_description_ids)
    selected = table.query(f"FIELD_ID=={field} && DATA_DESC_ID IN [{ids}]")
    return table, selected


def digest_array(digest: hashlib._Hash, values: np.ndarray) -> None:
    contiguous = np.ascontiguousarray(values)
    digest.update(contiguous.dtype.str.encode())
    digest.update(np.asarray(contiguous.shape, dtype=np.int64).tobytes())
    digest.update(contiguous.tobytes())


def compare(args: argparse.Namespace) -> dict[str, object]:
    windows = parse_spectral_windows(args.spw)
    ddids = selected_data_description_ids(args.source, windows)
    if not ddids:
        raise RuntimeError("selection resolves to no DATA_DESCRIPTION rows")
    opened = [
        open_selection(path, args.field, ddids)
        for path in (args.source, args.rust, args.casa)
    ]
    try:
        row_counts = [selection.nrows() for _, selection in opened]
        if len(set(row_counts)) != 1:
            raise RuntimeError(f"selected row counts differ: {row_counts}")
        selected_rows = row_counts[0]
        if selected_rows == 0:
            raise RuntimeError("selection resolves to no MAIN rows")
        source_cell_shape = np.asarray(opened[0][1].getcell("DATA", 0)).shape
        if len(source_cell_shape) != 2 or source_cell_shape[0] == 0:
            raise RuntimeError(f"unexpected DATA cell shape: {source_cell_shape}")
        correlation_end = source_cell_shape[0] - 1
        numerator = 0.0
        denominator = 0.0
        rust_power = 0.0
        cross_power = 0.0j
        conjugate_cross_power = 0.0j
        correlation_numerators = np.zeros(source_cell_shape[0], dtype=np.float64)
        correlation_denominators = np.zeros(source_cell_shape[0], dtype=np.float64)
        channel_numerators = np.zeros(args.channel_count, dtype=np.float64)
        channel_denominators = np.zeros(args.channel_count, dtype=np.float64)
        row_chunk_diagnostics: list[dict[str, object]] = []
        finite = True
        rust_flag_bitwise_unchanged = True
        rust_weight_bitwise_unchanged = True
        rust_sigma_bitwise_unchanged = True
        casa_flag_bitwise_unchanged = True
        casa_weight_bitwise_unchanged = True
        casa_sigma_bitwise_unchanged = True
        casa_weight_finite = True
        casa_weight_within_float32_roundtrip_tolerance = True
        casa_weight_maximum_absolute_difference = 0.0
        casa_weight_maximum_relative_difference = 0.0
        source_digest = hashlib.sha256()
        rust_digest = hashlib.sha256()
        casa_digest = hashlib.sha256()
        end_channel = args.channel_start + args.channel_count - 1
        for start_row in range(0, selected_rows, args.row_chunk):
            row_count = min(args.row_chunk, selected_rows - start_row)
            source = opened[0][1]
            rust = opened[1][1]
            casa = opened[2][1]
            rust_model = np.asarray(
                rust.getcolslice(
                    args.rust_column,
                    [0, args.channel_start],
                    [correlation_end, end_channel],
                    [],
                    start_row,
                    row_count,
                )
            )
            casa_model = np.asarray(
                casa.getcolslice(
                    args.casa_column,
                    [0, args.channel_start],
                    [correlation_end, end_channel],
                    [],
                    start_row,
                    row_count,
                )
            )
            finite = (
                finite
                and bool(np.all(np.isfinite(rust_model)))
                and bool(np.all(np.isfinite(casa_model)))
            )
            delta = rust_model - casa_model
            numerator += float(np.vdot(delta.ravel(), delta.ravel()).real)
            denominator += float(np.vdot(casa_model.ravel(), casa_model.ravel()).real)
            rust_power += float(np.vdot(rust_model.ravel(), rust_model.ravel()).real)
            cross_power += complex(np.vdot(casa_model.ravel(), rust_model.ravel()))
            conjugate_cross_power += complex(
                np.vdot(np.conjugate(casa_model).ravel(), rust_model.ravel())
            )
            for correlation in range(source_cell_shape[0]):
                correlation_delta = delta[correlation].ravel()
                correlation_casa = casa_model[correlation].ravel()
                correlation_numerators[correlation] += float(
                    np.vdot(correlation_delta, correlation_delta).real
                )
                correlation_denominators[correlation] += float(
                    np.vdot(correlation_casa, correlation_casa).real
                )
            for channel in range(args.channel_count):
                channel_delta = delta[:, channel, :].ravel()
                channel_casa = casa_model[:, channel, :].ravel()
                channel_numerators[channel] += float(
                    np.vdot(channel_delta, channel_delta).real
                )
                channel_denominators[channel] += float(
                    np.vdot(channel_casa, channel_casa).real
                )
            chunk_numerator = float(np.vdot(delta.ravel(), delta.ravel()).real)
            chunk_denominator = float(
                np.vdot(casa_model.ravel(), casa_model.ravel()).real
            )
            chunk_rust_power = float(
                np.vdot(rust_model.ravel(), rust_model.ravel()).real
            )
            chunk_cross_power = complex(np.vdot(casa_model.ravel(), rust_model.ravel()))
            row_chunk_diagnostics.append(
                {
                    "start_row": start_row,
                    "row_count": row_count,
                    "normalized_rms": math.sqrt(chunk_numerator / chunk_denominator)
                    if chunk_denominator > 0.0
                    else math.inf,
                    "rust_to_casa_l2_norm": math.sqrt(
                        chunk_rust_power / chunk_denominator
                    )
                    if chunk_denominator > 0.0
                    else math.inf,
                    "complex_alignment": abs(chunk_cross_power)
                    / math.sqrt(chunk_rust_power * chunk_denominator)
                    if chunk_rust_power > 0.0 and chunk_denominator > 0.0
                    else 0.0,
                }
            )
            digest_array(rust_digest, rust_model)
            digest_array(casa_digest, casa_model)
            source_flag = np.asarray(
                source.getcolslice(
                    "FLAG",
                    [0, args.channel_start],
                    [correlation_end, end_channel],
                    [],
                    start_row,
                    row_count,
                )
            )
            rust_flag = np.asarray(
                rust.getcolslice(
                    "FLAG",
                    [0, args.channel_start],
                    [correlation_end, end_channel],
                    [],
                    start_row,
                    row_count,
                )
            )
            casa_flag = np.asarray(
                casa.getcolslice(
                    "FLAG",
                    [0, args.channel_start],
                    [correlation_end, end_channel],
                    [],
                    start_row,
                    row_count,
                )
            )
            source_weight = np.asarray(source.getcol("WEIGHT", start_row, row_count))
            rust_weight = np.asarray(rust.getcol("WEIGHT", start_row, row_count))
            casa_weight = np.asarray(casa.getcol("WEIGHT", start_row, row_count))
            source_sigma = np.asarray(source.getcol("SIGMA", start_row, row_count))
            rust_sigma = np.asarray(rust.getcol("SIGMA", start_row, row_count))
            casa_sigma = np.asarray(casa.getcol("SIGMA", start_row, row_count))

            for source_values in (source_flag, source_weight, source_sigma):
                digest_array(source_digest, source_values)

            rust_flag_bitwise_unchanged = (
                rust_flag_bitwise_unchanged
                and arrays_bitwise_equal(source_flag, rust_flag)
            )
            rust_weight_bitwise_unchanged = (
                rust_weight_bitwise_unchanged
                and arrays_bitwise_equal(source_weight, rust_weight)
            )
            rust_sigma_bitwise_unchanged = (
                rust_sigma_bitwise_unchanged
                and arrays_bitwise_equal(source_sigma, rust_sigma)
            )
            casa_flag_bitwise_unchanged = (
                casa_flag_bitwise_unchanged
                and arrays_bitwise_equal(source_flag, casa_flag)
            )
            casa_weight_bitwise_unchanged = (
                casa_weight_bitwise_unchanged
                and arrays_bitwise_equal(source_weight, casa_weight)
            )
            casa_sigma_bitwise_unchanged = (
                casa_sigma_bitwise_unchanged
                and arrays_bitwise_equal(source_sigma, casa_sigma)
            )
            casa_weight_metrics = float32_weight_roundtrip_metrics(
                source_weight, casa_weight
            )
            casa_weight_finite = casa_weight_finite and bool(
                casa_weight_metrics["finite"]
            )
            casa_weight_within_float32_roundtrip_tolerance = (
                casa_weight_within_float32_roundtrip_tolerance
                and bool(casa_weight_metrics["within_tolerance"])
            )
            casa_weight_maximum_absolute_difference = max(
                casa_weight_maximum_absolute_difference,
                float(casa_weight_metrics["maximum_absolute_difference"]),
            )
            casa_weight_maximum_relative_difference = max(
                casa_weight_maximum_relative_difference,
                float(casa_weight_metrics["maximum_relative_difference"]),
            )
        nrms = math.sqrt(numerator / denominator) if denominator > 0.0 else math.inf
        best_scale = (
            cross_power / denominator
            if denominator > 0.0
            else complex(math.nan, math.nan)
        )
        best_scaled_residual = (
            max(0.0, rust_power - (abs(cross_power) ** 2 / denominator))
            if denominator > 0.0
            else math.inf
        )
        best_conjugate_residual = (
            max(0.0, rust_power - (abs(conjugate_cross_power) ** 2 / denominator))
            if denominator > 0.0
            else math.inf
        )
        relative_norm = (
            math.sqrt(rust_power / denominator) if denominator > 0.0 else math.inf
        )
        alignment = (
            abs(cross_power) / math.sqrt(rust_power * denominator)
            if rust_power > 0.0 and denominator > 0.0
            else 0.0
        )
        per_correlation_nrms = [
            math.sqrt(numerator / reference) if reference > 0.0 else math.inf
            for numerator, reference in zip(
                correlation_numerators, correlation_denominators, strict=True
            )
        ]
        per_channel_nrms = [
            math.sqrt(numerator / reference) if reference > 0.0 else math.inf
            for numerator, reference in zip(
                channel_numerators, channel_denominators, strict=True
            )
        ]
        casa_weight_accepted = casa_weight_contract_satisfied(
            args.comparison_key,
            bitwise_unchanged=casa_weight_bitwise_unchanged,
            within_float32_roundtrip_tolerance=(
                casa_weight_within_float32_roundtrip_tolerance
            ),
        )
        flags_unchanged = rust_flag_bitwise_unchanged and casa_flag_bitwise_unchanged
        weights_unchanged = rust_weight_bitwise_unchanged and casa_weight_accepted
        sigmas_unchanged = rust_sigma_bitwise_unchanged and casa_sigma_bitwise_unchanged
        passed = (
            finite
            and flags_unchanged
            and weights_unchanged
            and sigmas_unchanged
            and nrms <= args.maximum_nrms
        )
        comparison_schema = (
            "casa-rs-model-data-comparison-v1"
            if args.comparison_key == "model_data"
            else "casa-rs-continuum-residual-comparison-v1"
        )
        return {
            "schema": comparison_schema,
            "status": "pass" if passed else "fail",
            "selection": {
                "field": args.field,
                "spectral_windows": sorted(windows),
                "channel_start": args.channel_start,
                "channel_count": args.channel_count,
                "selected_rows": selected_rows,
                "selected_correlation_channel_samples": int(
                    rust_model.shape[0] * args.channel_count * selected_rows
                ),
            },
            args.comparison_key: {
                "rust_column": args.rust_column,
                "casa_column": args.casa_column,
                "normalized_rms": nrms,
                "maximum_normalized_rms": args.maximum_nrms,
                "diagnostics": {
                    "rust_to_casa_l2_norm": relative_norm,
                    "complex_alignment": alignment,
                    "best_fit_complex_scale": {
                        "real": best_scale.real,
                        "imaginary": best_scale.imag,
                    },
                    "best_fit_scaled_normalized_rms": math.sqrt(
                        best_scaled_residual / denominator
                    )
                    if denominator > 0.0
                    else math.inf,
                    "best_fit_conjugated_normalized_rms": math.sqrt(
                        best_conjugate_residual / denominator
                    )
                    if denominator > 0.0
                    else math.inf,
                    "per_correlation_normalized_rms": per_correlation_nrms,
                    "per_channel_normalized_rms": per_channel_nrms,
                    "row_chunks": row_chunk_diagnostics,
                },
                "finite": finite,
                "rust_sha256": rust_digest.hexdigest(),
                "casa_sha256": casa_digest.hexdigest(),
            },
            "source_columns": {
                "flag_unchanged": flags_unchanged,
                "weight_unchanged": weights_unchanged,
                "sigma_unchanged": sigmas_unchanged,
                "rust": {
                    "flag_bitwise_unchanged": rust_flag_bitwise_unchanged,
                    "weight_bitwise_unchanged": rust_weight_bitwise_unchanged,
                    "sigma_bitwise_unchanged": rust_sigma_bitwise_unchanged,
                },
                "casa": {
                    "flag_bitwise_unchanged": casa_flag_bitwise_unchanged,
                    "weight_bitwise_unchanged": casa_weight_bitwise_unchanged,
                    "sigma_bitwise_unchanged": casa_sigma_bitwise_unchanged,
                    "weight_finite": casa_weight_finite,
                    "weight_within_float32_roundtrip_tolerance": (
                        casa_weight_within_float32_roundtrip_tolerance
                    ),
                    "weight_maximum_absolute_difference": (
                        casa_weight_maximum_absolute_difference
                    ),
                    "weight_maximum_relative_difference": (
                        casa_weight_maximum_relative_difference
                    ),
                },
                "casa_weight_contract": (
                    "float32_roundtrip"
                    if args.comparison_key == "continuum_residual"
                    else "bitwise_exact"
                ),
                "float32_weight_roundtrip_tolerance": {
                    "relative": FLOAT32_WEIGHT_ROUNDTRIP_RELATIVE_TOLERANCE,
                    "absolute": FLOAT32_WEIGHT_ROUNDTRIP_ABSOLUTE_TOLERANCE,
                },
                "selection_digest_sha256": source_digest.hexdigest(),
            },
            "reopen_succeeded": True,
        }
    finally:
        for table, selected in opened:
            selected.close()
            table.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--casa", type=Path, required=True)
    parser.add_argument("--field", type=int, required=True)
    parser.add_argument("--spw", required=True)
    parser.add_argument("--channel-start", type=int, required=True)
    parser.add_argument("--channel-count", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rust-column", default="MODEL_DATA")
    parser.add_argument("--casa-column", default="MODEL_DATA")
    parser.add_argument(
        "--comparison-key",
        choices=("model_data", "continuum_residual"),
        default="model_data",
    )
    parser.add_argument("--maximum-nrms", type=float, default=0.001)
    parser.add_argument("--row-chunk", type=int, default=2048)
    args = parser.parse_args()
    result = compare(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(
        f"{args.comparison_key}_comparison={args.output} "
        f"status={result['status']} "
        f"nrms={result[args.comparison_key]['normalized_rms']:.9e}"
    )
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
