#!/usr/bin/env python3
"""Compare representative Rust/CASA visibility-column writes without loading a full MS."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
from casatools import table as table_tool


def selected_data_description_ids(ms: Path, spectral_windows: set[int]) -> list[int]:
    table = table_tool()
    try:
        table.open(str(ms / "DATA_DESCRIPTION"), nomodify=True)
        values = np.asarray(table.getcol("SPECTRAL_WINDOW_ID"), dtype=np.int64)
    finally:
        table.close()
    return [int(index) for index, value in enumerate(values) if int(value) in spectral_windows]


def parse_spectral_windows(selection: str) -> set[int]:
    result: set[int] = set()
    for item in selection.split(","):
        bounds = item.split(":", 1)[0].split("~")
        start = int(bounds[0])
        end = int(bounds[-1])
        result.update(range(start, end + 1))
    return result


def open_selection(ms: Path, field: int, data_description_ids: list[int]):
    table = table_tool()
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
    opened = [open_selection(path, args.field, ddids) for path in (args.source, args.rust, args.casa)]
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
        finite = True
        flags_equal = True
        weights_equal = True
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
                rust.getcolslice(args.rust_column, [0, args.channel_start], [correlation_end, end_channel], [], start_row, row_count)
            )
            casa_model = np.asarray(
                casa.getcolslice(args.casa_column, [0, args.channel_start], [correlation_end, end_channel], [], start_row, row_count)
            )
            finite = finite and bool(np.all(np.isfinite(rust_model))) and bool(np.all(np.isfinite(casa_model)))
            delta = rust_model - casa_model
            numerator += float(np.vdot(delta.ravel(), delta.ravel()).real)
            denominator += float(np.vdot(casa_model.ravel(), casa_model.ravel()).real)
            digest_array(rust_digest, rust_model)
            digest_array(casa_digest, casa_model)
            for column in ("FLAG", "WEIGHT"):
                if column == "FLAG":
                    source_values = np.asarray(source.getcolslice(column, [0, args.channel_start], [correlation_end, end_channel], [], start_row, row_count))
                    rust_values = np.asarray(rust.getcolslice(column, [0, args.channel_start], [correlation_end, end_channel], [], start_row, row_count))
                    casa_values = np.asarray(casa.getcolslice(column, [0, args.channel_start], [correlation_end, end_channel], [], start_row, row_count))
                else:
                    source_values = np.asarray(source.getcol(column, start_row, row_count))
                    rust_values = np.asarray(rust.getcol(column, start_row, row_count))
                    casa_values = np.asarray(casa.getcol(column, start_row, row_count))
                digest_array(source_digest, source_values)
                flags_equal = flags_equal and (column != "FLAG" or (np.array_equal(source_values, rust_values) and np.array_equal(source_values, casa_values)))
                weights_equal = weights_equal and (column != "WEIGHT" or (np.array_equal(source_values, rust_values) and np.array_equal(source_values, casa_values)))
        nrms = math.sqrt(numerator / denominator) if denominator > 0.0 else math.inf
        passed = finite and flags_equal and weights_equal and nrms <= args.maximum_nrms
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
                "selected_correlation_channel_samples": int(rust_model.shape[0] * args.channel_count * selected_rows),
            },
            args.comparison_key: {
                "rust_column": args.rust_column,
                "casa_column": args.casa_column,
                "normalized_rms": nrms,
                "maximum_normalized_rms": args.maximum_nrms,
                "finite": finite,
                "rust_sha256": rust_digest.hexdigest(),
                "casa_sha256": casa_digest.hexdigest(),
            },
            "source_columns": {
                "flag_unchanged": flags_equal,
                "weight_unchanged": weights_equal,
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
