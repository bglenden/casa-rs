#!/usr/bin/env python3
"""Summarize MS row/channel selection for CASA-vs-casa-rs imaging runs.

This probe deliberately stops before imaging. It answers the cheap equivalence
questions that must be true before a tclean/casa-rs timing comparison is useful:
which rows, fields, DDIDs, SPWs, source channels, flags, and raw weights are in
scope for the requested one-plane workload.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from typing import Any

import numpy as np
from casatools import table


def parse_int_list(text: str) -> set[int]:
    values: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "~" in part:
            start, end = part.split("~", 1)
            values.update(range(int(start), int(end) + 1))
        else:
            values.add(int(part))
    return values


def as_list(value: Any) -> list[Any]:
    if hasattr(value, "tolist"):
        return value.tolist()
    return list(value)


def read_ddid_to_spw(ms_path: str) -> tuple[list[int], list[int]]:
    tb = table()
    tb.open(os.path.join(ms_path, "DATA_DESCRIPTION"))
    try:
        spw = [int(value) for value in as_list(tb.getcol("SPECTRAL_WINDOW_ID"))]
        pol = [int(value) for value in as_list(tb.getcol("POLARIZATION_ID"))]
    finally:
        tb.close()
    return spw, pol


def read_spw_channels(ms_path: str, spw: int) -> tuple[np.ndarray, np.ndarray]:
    tb = table()
    tb.open(os.path.join(ms_path, "SPECTRAL_WINDOW"))
    try:
        freqs = np.asarray(tb.getcell("CHAN_FREQ", spw), dtype=np.float64)
        widths = np.asarray(tb.getcell("CHAN_WIDTH", spw), dtype=np.float64)
    finally:
        tb.close()
    return freqs, widths


def read_pol_corr_types(ms_path: str, pol_id: int) -> list[int]:
    tb = table()
    tb.open(os.path.join(ms_path, "POLARIZATION"))
    try:
        corr_type = np.asarray(tb.getcell("CORR_TYPE", pol_id), dtype=np.int64)
    finally:
        tb.close()
    return [int(value) for value in corr_type]


def stokes_i_parallel_hand_indices(corr_types: list[int]) -> tuple[int, int]:
    """Return the CASA Stokes-I parallel-hand pair for this polarization basis."""

    for first_type, second_type in ((5, 8), (9, 12)):  # RR/LL, XX/YY
        if first_type in corr_types and second_type in corr_types:
            return corr_types.index(first_type), corr_types.index(second_type)
    raise RuntimeError(
        "cannot derive strict Stokes-I parallel hands from CORR_TYPE "
        f"{corr_types}; expected RR/LL or XX/YY"
    )


def selected_source_channels(
    args: argparse.Namespace, spw_channel_count: int
) -> list[int]:
    if args.specmode == "cube":
        start = args.start if args.start is not None else args.channel_start
        width = args.width if args.width is not None else 1
        nchan = args.channel_count
        indices: list[int] = []
        for output in range(nchan):
            first = start + output * width
            indices.extend(range(first, first + width))
        return [index for index in indices if 0 <= index < spw_channel_count]
    first = args.channel_start
    return list(range(first, min(first + args.channel_count, spw_channel_count)))


def field_counter(values: np.ndarray) -> dict[str, int]:
    unique, counts = np.unique(values.astype(np.int64), return_counts=True)
    return {str(int(field)): int(count) for field, count in zip(unique, counts)}


def summarize(args: argparse.Namespace) -> dict[str, Any]:
    ddid_to_spw, ddid_to_pol = read_ddid_to_spw(args.ms)
    freqs, widths = read_spw_channels(args.ms, args.spw)
    source_channels = selected_source_channels(args, len(freqs))
    if not source_channels:
        raise SystemExit("selected source-channel set is empty")
    source_start = min(source_channels)
    source_end = max(source_channels)
    selected_fields = parse_int_list(args.field)
    selected_spws = {args.spw}
    selected_ddids = {
        ddid
        for ddid, spw in enumerate(ddid_to_spw)
        if spw in selected_spws and ddid < len(ddid_to_pol)
    }
    corr_types = read_pol_corr_types(args.ms, ddid_to_pol[min(selected_ddids)])
    corr_count = len(corr_types)
    parallel_hand_indices = stokes_i_parallel_hand_indices(corr_types)
    source_channel_offsets = [
        source_channel - source_start for source_channel in source_channels
    ]

    tb = table()
    tb.open(args.ms)
    try:
        nrows = int(tb.nrows())
        field_id = np.asarray(tb.getcol("FIELD_ID"), dtype=np.int64)
        ddid = np.asarray(tb.getcol("DATA_DESC_ID"), dtype=np.int64)
        flag_row = np.asarray(tb.getcol("FLAG_ROW"), dtype=bool)
        selected_mask = np.isin(field_id, list(selected_fields)) & np.isin(
            ddid, list(selected_ddids)
        )
        active_mask = selected_mask & ~flag_row

        by_field_selected = field_counter(field_id[selected_mask])
        by_field_active = field_counter(field_id[active_mask])
        by_ddid_selected = field_counter(ddid[selected_mask])
        by_ddid_active = field_counter(ddid[active_mask])

        block_rows = args.block_rows
        rows_with_any_channel_flag = 0
        rows_with_all_source_channels_clear = 0
        clear_corr_channel_cells = 0
        total_corr_channel_cells = 0
        raw_weight_sum = 0.0
        raw_weight_min = None
        raw_weight_max = None
        by_field_any_channel_flag: dict[str, int] = defaultdict(int)
        by_field_clear_rows: dict[str, int] = defaultdict(int)
        by_field_clear_corr_channel_cells: dict[str, int] = defaultdict(int)
        by_field_total_corr_channel_cells: dict[str, int] = defaultdict(int)
        by_field_attempted_stokes_i_samples: dict[str, int] = defaultdict(int)
        by_field_accepted_stokes_i_samples: dict[str, int] = defaultdict(int)
        attempted_stokes_i_samples = 0
        accepted_stokes_i_samples = 0

        for start_row in range(0, nrows, block_rows):
            nblock = min(block_rows, nrows - start_row)
            block_active = active_mask[start_row : start_row + nblock]
            if not np.any(block_active):
                continue
            block_fields = field_id[start_row : start_row + nblock]

            flag = tb.getcolslice(
                "FLAG",
                [0, source_start],
                [corr_count - 1, source_end],
                [],
                start_row,
                nblock,
            )
            flag = np.asarray(flag, dtype=bool)
            if flag.ndim != 3:
                raise RuntimeError(f"unexpected FLAG block shape {flag.shape}")
            if flag.shape[2] == nblock:
                flag_by_row = np.moveaxis(flag, 2, 0)
            else:
                flag_by_row = flag
            active_flags = flag_by_row[block_active][:, :, source_channel_offsets]
            row_any_flag = np.any(active_flags, axis=(1, 2))
            accepted_by_row_channel = ~np.any(
                active_flags[:, parallel_hand_indices, :], axis=1
            )
            rows_with_any_channel_flag += int(np.count_nonzero(row_any_flag))
            rows_with_all_source_channels_clear += int(
                row_any_flag.size - np.count_nonzero(row_any_flag)
            )
            clear_corr_channel_cells += int(np.count_nonzero(~active_flags))
            total_corr_channel_cells += int(active_flags.size)
            attempted_stokes_i_samples += int(accepted_by_row_channel.size)
            accepted_stokes_i_samples += int(np.count_nonzero(accepted_by_row_channel))

            active_fields = block_fields[block_active]
            for field, flagged, row_flags, row_accepted in zip(
                active_fields,
                row_any_flag,
                active_flags,
                accepted_by_row_channel,
            ):
                key = str(int(field))
                if bool(flagged):
                    by_field_any_channel_flag[key] += 1
                else:
                    by_field_clear_rows[key] += 1
                by_field_clear_corr_channel_cells[key] += int(
                    np.count_nonzero(~row_flags)
                )
                by_field_total_corr_channel_cells[key] += int(row_flags.size)
                by_field_attempted_stokes_i_samples[key] += int(row_accepted.size)
                by_field_accepted_stokes_i_samples[key] += int(
                    np.count_nonzero(row_accepted)
                )

            weight = np.asarray(
                tb.getcol("WEIGHT", start_row, nblock), dtype=np.float64
            )
            if weight.ndim == 2 and weight.shape[1] == nblock:
                weight_by_row = weight.T
            elif weight.ndim == 2 and weight.shape[0] == nblock:
                weight_by_row = weight
            else:
                raise RuntimeError(f"unexpected WEIGHT block shape {weight.shape}")
            active_weight = weight_by_row[block_active, :corr_count]
            raw_weight_sum += float(np.sum(active_weight))
            block_min = float(np.min(active_weight)) if active_weight.size else None
            block_max = float(np.max(active_weight)) if active_weight.size else None
            if block_min is not None:
                raw_weight_min = (
                    block_min
                    if raw_weight_min is None
                    else min(raw_weight_min, block_min)
                )
                raw_weight_max = (
                    block_max
                    if raw_weight_max is None
                    else max(raw_weight_max, block_max)
                )
    finally:
        tb.close()

    return {
        "schema_version": 2,
        "probe": "ms_selection_accounting",
        "ms": args.ms,
        "request": {
            "field": args.field,
            "spw": args.spw,
            "specmode": args.specmode,
            "channel_start": args.channel_start,
            "channel_count": args.channel_count,
            "start": args.start,
            "width": args.width,
        },
        "selection": {
            "total_rows": nrows,
            "selected_rows": int(np.count_nonzero(selected_mask)),
            "active_rows": int(np.count_nonzero(active_mask)),
            "rows_skipped_by_flag_row": int(np.count_nonzero(selected_mask & flag_row)),
            "selected_fields": sorted(selected_fields),
            "selected_ddids": sorted(int(value) for value in selected_ddids),
            "by_field_selected_rows": by_field_selected,
            "by_field_active_rows": by_field_active,
            "by_ddid_selected_rows": by_ddid_selected,
            "by_ddid_active_rows": by_ddid_active,
        },
        "spectral": {
            "spw_channel_count": int(len(freqs)),
            "source_channel_count": int(len(source_channels)),
            "source_channel_start": int(source_start),
            "source_channel_end": int(source_end),
            "source_frequency_start_hz": float(freqs[source_start]),
            "source_frequency_end_hz": float(freqs[source_end]),
            "source_width_sum_hz": float(np.sum(widths[source_start : source_end + 1])),
            "corr_count": corr_count,
            "corr_types": corr_types,
            "stokes_i_parallel_hand_indices": list(parallel_hand_indices),
            "output_channel_count": args.channel_count,
        },
        "flags": {
            "active_rows_with_any_source_channel_flag": rows_with_any_channel_flag,
            "active_rows_with_all_source_channels_clear": rows_with_all_source_channels_clear,
            "clear_corr_channel_cells": clear_corr_channel_cells,
            "total_corr_channel_cells": total_corr_channel_cells,
            "clear_corr_channel_fraction": (
                clear_corr_channel_cells / total_corr_channel_cells
                if total_corr_channel_cells
                else None
            ),
            "by_field_any_source_channel_flag_rows": dict(
                sorted(by_field_any_channel_flag.items())
            ),
            "by_field_all_source_channels_clear_rows": dict(
                sorted(by_field_clear_rows.items())
            ),
            "by_field_clear_corr_channel_cells": dict(
                sorted(by_field_clear_corr_channel_cells.items())
            ),
            "by_field_total_corr_channel_cells": dict(
                sorted(by_field_total_corr_channel_cells.items())
            ),
        },
        "samples": {
            "definition": (
                "one attempted Stokes-I visibility sample per active row and "
                "selected source channel; accepted when both CASA-selected "
                "parallel-hand correlation cells are clear"
            ),
            "attempted_stokes_i_samples": attempted_stokes_i_samples,
            "accepted_stokes_i_samples": accepted_stokes_i_samples,
            "by_field_attempted_stokes_i_samples": dict(
                sorted(by_field_attempted_stokes_i_samples.items())
            ),
            "by_field_accepted_stokes_i_samples": dict(
                sorted(by_field_accepted_stokes_i_samples.items())
            ),
        },
        "weights": {
            "raw_row_weight_sum_active_rows": raw_weight_sum,
            "raw_row_weight_min": raw_weight_min,
            "raw_row_weight_max": raw_weight_max,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ms", required=True)
    parser.add_argument("--field", default="0")
    parser.add_argument("--spw", type=int, default=0)
    parser.add_argument("--specmode", choices=["mfs", "cube"], default="mfs")
    parser.add_argument("--channel-start", type=int, default=0)
    parser.add_argument("--channel-count", type=int, default=1)
    parser.add_argument("--start", type=int)
    parser.add_argument("--width", type=int)
    parser.add_argument("--block-rows", type=int, default=32768)
    args = parser.parse_args()
    print(json.dumps(summarize(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
